//! SP31: explicit type casts, `CAST(expr AS type)` and `expr::type`.
//!
//! This is the *explicit* cast context (the broadest PostgreSQL cast context),
//! among the slice's five runtime types (`bool`, `int4`, `int8`, `text`,
//! `float8`). It is a pure value transform with no I/O, no catalog and no
//! concurrency, so it lives here in the type layer, and unit tests prove it
//! exhaustively.
//!
//! Two entry points share one cast matrix:
//!   * [`cast_allowed`][]: a *static* (plan-time) predicate on `(from, to)` column
//!     types, so [`crate::ops`]-free callers can reject an undefined cast with
//!     SQLSTATE 42846 before any row is produced (and so `RowDescription` knows
//!     the result type).
//!   * [`cast`][]: the *runtime* value conversion of one (possibly NULL) `Datum`.
//!
//! The defined casts (NULL → NULL for every one of them):
//!   * identity `T → T`;
//!   * numeric ↔ numeric (`int4`/`int8`/`float8`, any direction): widening,
//!     range-checked narrowing (22003), and `float8 → int` rounding half-to-even;
//!   * `bool → int4` (`false`→0, `true`→1) and `int4 → bool` (0→false, else true).
//!     PostgreSQL has these only for `int4`, not `int8`;
//!   * any type `→ text` (the type's output text), and `text →` any type (parsed,
//!     22P02 on bad syntax, 22003 on overflow).
//!
//! Everything else (e.g. `float8`/`int8` ↔ `bool`) is undefined → 42846.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible cast semantics kept structurally close to donor"
)]

use crate::{ColumnType, Datum, TypeError, string::Coercion};

/// `polygon(circle)` is spelled `select pg_catalog.polygon(12, $1)` in
/// `pg_proc`, so the `circle → polygon` cast always produces twelve vertices.
const CIRCLE_CAST_VERTICES: i32 = 12;

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
        // jsonpath is a distinct scalar type with text input/output functions.
        (ColumnType::Text, ColumnType::JsonPath) | (ColumnType::JsonPath, ColumnType::Text) => true,
        // A domain casts exactly as its base type does, in both directions:
        // `PostgreSQL` coerces through the base and then applies the domain's
        // constraints. This arm must come first so a domain never falls into
        // the string rules below on the strength of its *name*.
        // A cast the user declared reaches whatever pair it names, and it is
        // consulted before the built-in rules only for the pairs those rules
        // would otherwise resolve by name — a user type has no such name.
        _ if crate::usercast::any_declared()
            && crate::usercast::is_declared(from.oid(), to.oid()) =>
        {
            true
        }
        (ColumnType::Domain(d), _) => cast_allowed(*d.base, to),
        (_, ColumnType::Domain(d)) => cast_allowed(from, *d.base),
        // A user-defined base type inherits *nothing*. `CREATE TYPE … (LIKE =
        // float4)` copies float4's storage, not its conversions, so the only
        // route in or out is a `CREATE CAST` the user wrote — which lives in
        // the catalog and is resolved a rung up, in the executor. This arm has
        // to precede the string fall-throughs below, or `xfloat4::text` would
        // be promised here and then fail at conversion.
        (ColumnType::Base(_), _) | (_, ColumnType::Base(_)) => {
            crate::usercast::is_declared(from.oid(), to.oid())
        }
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
        (ColumnType::Range(range), ColumnType::Multirange(multirange)) => range == multirange.range,
        // Array → array whenever the element cast is defined (PostgreSQL builds
        // an array coercion from the element coercion). Must precede the string
        // rules so `text[] → int4[]` is judged element-wise, not as a string cast.
        (ColumnType::Array(a), ColumnType::Array(b)) => {
            cast_allowed(a.column_type(), b.column_type())
        }
        (ColumnType::OidVector, ColumnType::Array(crate::ElemType::Regtype)) => true,
        // The fourteen geometric conversions `pg_cast` declares, and no others:
        // there is no `point → polygon`, no `lseg → box`, and nothing at all
        // touching `line`. Each runs a named conversion function, so the pairs
        // are listed one by one rather than being derived from "both sides are
        // geometric".
        _ if geometric_cast(from, to) => true,
        // The system identifier family. `pg_cast` gives `oid` conversions with
        // the integer types and with every `reg*` type, and gives `xid8 → xid`
        // (explicit); it gives the other four nothing at all, so they reach any
        // other type only through the string rules below. The whole family must
        // be decided here, ahead of the numeric fall-throughs, or `oid` would
        // silently acquire `oid → numeric` and `float8 → oid`, which PostgreSQL
        // reports as 42846.
        //
        // The integer directions are not symmetric: `int2 → oid` exists but
        // `oid → int2` does not.
        (ColumnType::Int2 | Int4 | ColumnType::Int8, ColumnType::Oid)
        | (ColumnType::Oid, Int4 | ColumnType::Int8)
        | (ColumnType::Xid8, ColumnType::Xid) => true,
        (ColumnType::Oid, reg) | (reg, ColumnType::Oid) if reg.is_reg() => true,
        (
            ColumnType::Oid
            | ColumnType::Xid
            | ColumnType::Xid8
            | ColumnType::Cid
            | ColumnType::Tid
            | ColumnType::PgLsn,
            _,
        )
        | (
            _,
            ColumnType::Oid
            | ColumnType::Xid
            | ColumnType::Xid8
            | ColumnType::Cid
            | ColumnType::Tid
            | ColumnType::PgLsn,
        ) => from.is_string() || to.is_string(),
        // `pg_snapshot` and `txid_snapshot` have no `pg_cast` entry, so each
        // reaches another type only through its text form. The pair is decided
        // here for the same reason the family above is: without this arm they
        // would fall into the string rules from the far side and acquire
        // conversions PostgreSQL refuses.
        //
        // Relabelling one as the other is allowed, which PostgreSQL does not
        // do. Gres holds both in one datum, so the conversion is a no-op it
        // has no way to fail at, and refusing it would only stop a value
        // reaching a column that already holds exactly that value.
        (
            ColumnType::PgSnapshot | ColumnType::TxidSnapshot,
            ColumnType::PgSnapshot | ColumnType::TxidSnapshot,
        ) => true,
        (ColumnType::PgSnapshot | ColumnType::TxidSnapshot, other)
        | (other, ColumnType::PgSnapshot | ColumnType::TxidSnapshot) => other.is_string(),
        // The network family's own casts (`pg_cast`): `cidr → inet` is
        // binary-coercible, `inet → cidr` runs `inet_to_cidr`, and the two MAC
        // widths convert both ways. Everything else in the family reaches
        // another type only through its text form, via the string rules below.
        (ColumnType::Cidr, ColumnType::Inet)
        | (ColumnType::Inet, ColumnType::Cidr)
        | (ColumnType::MacAddr, ColumnType::MacAddr8)
        | (ColumnType::MacAddr8, ColumnType::MacAddr) => true,
        // `bit` and `bit varying` are binary-coercible to each other in both
        // directions, and each re-coerces to its own type under a different
        // length modifier.
        (
            ColumnType::Bit(_) | ColumnType::VarBit(_),
            ColumnType::Bit(_) | ColumnType::VarBit(_),
        ) => true,
        // `pg_cast` gives `bit` explicit casts to and from `int4` and `int8`
        // ONLY — not `int2`, not `numeric`, and not from `bit varying`, whose
        // values reach an integer only by being relabelled `bit` first.
        (ColumnType::Bit(_), Int4 | ColumnType::Int8)
        | (Int4 | ColumnType::Int8, ColumnType::Bit(_)) => true,
        // `pg_cast` gives `money` exactly four conversions: from `int4`,
        // `int8` and `numeric`, and to `numeric`. There is deliberately no
        // `money → int`, no `money → float`, and no `float → money`.
        (Int4 | ColumnType::Int8 | ColumnType::Numeric(_), ColumnType::Money)
        | (ColumnType::Money, ColumnType::Numeric(_)) => true,
        (ColumnType::Money, _) | (_, ColumnType::Money) => from.is_string() || to.is_string(),
        // `pg_cast` gives `"char"` eight entries: to and from each of `text`,
        // `bpchar` and `varchar` (the string rule below), and to and from
        // `int4` explicitly. Nothing else — `'a'::"char"::float8` is 42846, and
        // this arm is what keeps the numeric fall-through from granting it.
        (ColumnType::InternalChar, Int4) | (Int4, ColumnType::InternalChar) => true,
        (ColumnType::InternalChar, _) | (_, ColumnType::InternalChar) => {
            from.is_string() || to.is_string()
        }
        // Everything else in the bit family converts only through its text
        // form, so it must not fall into the numeric rules further down.
        (ColumnType::Bit(_) | ColumnType::VarBit(_), _)
        | (_, ColumnType::Bit(_) | ColumnType::VarBit(_)) => from.is_string() || to.is_string(),
        // `pg_cast` gives `xml` six entries and no more: `text`, `varchar` and
        // `bpchar` each convert to it explicitly (via `xml(text)`, which is
        // `xml_in`) and back from it at assignment level, binary-coercibly.
        // There is deliberately no `xml → int`, no `xml → json`, and no
        // `xml → xml[]` — nothing outside the string family.
        (ColumnType::Xml, _) | (_, ColumnType::Xml) => from.is_string() || to.is_string(),
        // `pg_cast` has exactly two entries between the JSON types, both at
        // assignment level and both running the target's input function over the
        // source's output — which is why `'{"b":1,  "a":2}'::json::jsonb`
        // normalises and `…::jsonb::json` keeps `jsonb`'s canonical order.
        (ColumnType::Json, ColumnType::Jsonb) | (ColumnType::Jsonb, ColumnType::Json) => true,
        // `json`, `jsonb` and arrays otherwise interconvert ONLY with the string
        // family (the rule below): PostgreSQL has no json/jsonb/array ↔
        // number/bool/temporal cast, and this arm keeps the permissive numeric
        // rules from claiming one.
        (ColumnType::Json | ColumnType::Jsonb | ColumnType::Array(_), _)
        | (_, ColumnType::Json | ColumnType::Jsonb | ColumnType::Array(_))
            if !from.is_string() && !to.is_string() =>
        {
            false
        }
        _ if from.is_numeric() && to.is_numeric() => true,
        // Numeric family ↔ numeric family, any direction.
        _ if num_family(from) && num_family(to) => true,
        // PostgreSQL defines bool↔int only for int4 (not int8 / float8 / numeric).
        (Bool, Int4) | (Int4, Bool) => true,
        // The `reg*` family interconverts with the integer oid family;
        // text↔`reg*` is covered by the string rules below.
        (Int4 | ColumnType::Int8, reg) | (reg, Int4 | ColumnType::Int8) if reg.is_reg() => true,
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

/// The geometric ↔ geometric pairs `pg_cast` declares, each backed by a
/// conversion function of the target type's name (`polygon(box)`, `point(lseg)`,
/// …). Four are assignment-level ([`geometric_assignment_cast`]) and the rest
/// explicit; both contexts share this membership test.
///
/// `line` appears in none of them: PostgreSQL gives the infinite line no
/// conversion to or from any other geometric type. Neither does `point → lseg`,
/// `point → polygon`, `lseg → box` or `path → box` — the missing directions are
/// as load-bearing as the present ones, since each absence is a 42846.
fn geometric_cast(from: ColumnType, to: ColumnType) -> bool {
    use ColumnType::{Box, Circle, Lseg, Path, Point, Polygon};
    matches!(
        (from, to),
        (Point, Box)
            | (Lseg, Point)
            | (Path, Polygon)
            | (Box, Point | Lseg | Polygon | Circle)
            | (Polygon, Point | Path | Box | Circle)
            | (Circle, Point | Box | Polygon)
    )
}

/// The four geometric casts `pg_cast` marks `castcontext = 'a'`. The other ten
/// are `'e'`, so `INSERT`ing a `circle` into a `polygon` column still needs the
/// cast written out even though `circle::polygon` exists.
fn geometric_assignment_cast(from: ColumnType, to: ColumnType) -> bool {
    use ColumnType::{Box, Path, Point, Polygon};
    matches!(
        (from, to),
        (Point, Box) | (Path, Polygon) | (Box, Polygon) | (Polygon, Path)
    )
}

/// Is an *implicit-or-assignment* cast from `from` to `to` defined — the pairs
/// PostgreSQL 18's `pg_cast` marks `castcontext` `'i'` or `'a'`, restricted to
/// crabka's types? A strict SUBSET of [`cast_allowed`]: assignment (INSERT /
/// UPDATE SET into a column) converts through these pairs automatically, while
/// everything else keeps requiring an explicit `CAST`.
///
/// The allowed pairs and their `pg_cast` contexts:
///   * identity `T → T` (no cast needed);
///   * numeric family (`int4`/`int8`/`float8`/`numeric`) interconversion:
///     widenings are `'i'`, narrowings are `'a'`;
///   * string family (`text`/`varchar`/`char`) interconversion: `'i'`/`'a'`
///     (length re-coercion applies at assignment);
///   * `date → timestamp` and `date → timestamptz`: `'i'`;
///   * `timestamp → timestamptz`: `'i'`; `timestamptz → timestamp`: `'a'`
///     (both rotate through the session time zone).
///
/// Deliberately NOT allowed (explicit-only in this matrix):
///   * non-string ↔ string (PostgreSQL's I/O-conversion casts are
///     explicit-only since 8.3, so an `INSERT` of an `int4` into a `text` column
///     errors, and the reverse errors too);
///   * `bool ↔ int4` (`castcontext` `'e'`);
///   * `timestamp`/`timestamptz` → `date`/`time` (kept explicit-only here as a
///     conservative subset, though PostgreSQL marks these `'a'`);
///   * every pair with `interval`, `bytea`, `uuid` or `regclass` across
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
        // `pg_cast` marks `int2`/`int4`/`int8 → oid` implicit and `oid → int4`
        // and `oid → int8` assignment-level, so an integer stores into an `oid`
        // column and back into an integer one without an explicit cast. The
        // `reg*` pairs are all implicit binary coercions.
        (ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8, ColumnType::Oid)
        | (ColumnType::Oid, ColumnType::Int4 | ColumnType::Int8) => true,
        (ColumnType::Oid, reg) | (reg, ColumnType::Oid) if reg.is_reg() => true,
        // `pg_cast` marks `cidr → inet` and both MAC conversions implicit and
        // `inet → cidr` assignment-level, so `INSERT` into a column of the
        // other type converts without an explicit cast.
        (ColumnType::Cidr, ColumnType::Inet)
        | (ColumnType::Inet, ColumnType::Cidr)
        | (ColumnType::MacAddr, ColumnType::MacAddr8)
        | (ColumnType::MacAddr8, ColumnType::MacAddr) => true,
        // All four of `money`'s casts are `castcontext = 'a'`, so a `bigint`
        // column value stores into a `money` column and back into a `numeric`
        // one without an explicit cast.
        (ColumnType::Int4 | ColumnType::Int8 | ColumnType::Numeric(_), ColumnType::Money)
        | (ColumnType::Money, ColumnType::Numeric(_)) => true,
        // `"char"` is the one type outside the string family whose casts to and
        // from it are `'i'`/`'a'` rather than explicit, so `INSERT`ing a `text`
        // value into a `"char"` column converts, and takes its first byte. Its
        // `int4` pair stays explicit-only.
        (ColumnType::InternalChar, t) | (t, ColumnType::InternalChar) if t.is_string() => true,
        // `pg_cast` marks every bit-family conversion `'i'`, so storing a
        // `bit varying` in a `bit(n)` column needs no explicit cast — what it
        // does need is the length to match, which the coercion enforces.
        (
            ColumnType::Bit(_) | ColumnType::VarBit(_),
            ColumnType::Bit(_) | ColumnType::VarBit(_),
        ) => true,
        // `pg_snapshot` and `txid_snapshot` hold one value in one datum here,
        // so which of the two a value belongs to is the column's business and
        // not the value's. A store between them is therefore a relabelling
        // with nothing to convert and nothing to fail at, and it is admitted
        // rather than reported as the mismatch it is not. `PostgreSQL` keeps
        // them apart, having a separate representation for each; the
        // divergence is the store, never the reported type, which stays the
        // column's own.
        (
            ColumnType::PgSnapshot | ColumnType::TxidSnapshot,
            ColumnType::PgSnapshot | ColumnType::TxidSnapshot,
        ) => true,
        _ => geometric_assignment_cast(from, to),
    }
}

/// Do an explicit cast of a (possibly NULL) `Datum` to `to`. NULL casts to
/// NULL of the target type. A text-parse failure is 22P02; a numeric overflow is
/// 22003; an undefined `(from, to)` pair is 42846. Callers that gate on
/// [`cast_allowed`] at plan time never reach that last arm for a non-NULL value.
///
/// This function forwards `tz` to `encode_text` for the `* → text` cast arms
/// with `Timestamptz`; all other cast paths ignore it. Task 7 will add `text →
/// timestamptz` and will use `tz` for the parse.
pub fn cast(value: &Datum, to: ColumnType, tz: &jiff::tz::TimeZone) -> Result<Datum, TypeError> {
    cast_in(value, to, crate::encoding::OutputStyle::with_zone(tz))
}

/// [`cast`] under assignment rules rather than explicit-cast rules.
///
/// The two contexts differ only in how a `varchar(n)`/`char(n)` target treats an
/// over-long value: an explicit cast truncates it, and an assignment rejects it
/// with `string_data_right_truncation` unless the discarded characters are all
/// spaces. Use this wherever the engine *stores* a value, such as a column or a
/// routine parameter, and use [`cast`] for a cast the query wrote out.
///
/// # Errors
///
/// As [`cast`], plus 22001 for an over-long bounded-string assignment.
pub fn cast_assign(
    value: &Datum,
    to: ColumnType,
    tz: &jiff::tz::TimeZone,
) -> Result<Datum, TypeError> {
    cast_assign_in(value, to, crate::encoding::OutputStyle::with_zone(tz))
}

/// [`cast_assign`] in the session's styles, which is what storing a value
/// actually needs: `PostgreSQL` reaches an assignment cast through the target
/// type's *input function*, and that reads `DateStyle` exactly as a written
/// cast does. `INSERT INTO t VALUES ('97/02/10')` and `SELECT date '97/02/10'`
/// therefore agree about which field is the day under `DateStyle = 'ISO, YMD'`.
///
/// Prefer this over [`cast_assign`] wherever the session is at hand; the
/// zone-only spelling is for callers rendering a canonical value.
///
/// # Errors
///
/// As [`cast_assign`].
pub fn cast_assign_in(
    value: &Datum,
    to: ColumnType,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Datum, TypeError> {
    // Cast to the *unbounded* type first so `cast_in` applies no modifier of its
    // own, then apply the modifier under assignment rules. Splitting it this way
    // keeps one implementation of every cast arm.
    match to {
        ColumnType::Varchar(Some(_)) | ColumnType::Char(Some(_)) => {
            let unbounded = cast_in(value, unbounded_string(to), style)?;
            bounded_string(&unbounded, to)
        }
        // `bit(n)` rejects a length mismatch and `bit varying(n)` an over-long
        // value when the coercion is not explicit, which is the whole
        // difference between `B'10'::bit(11)` and storing `B'10'` in one.
        ColumnType::Bit(Some(len)) | ColumnType::VarBit(Some(len)) => {
            let varying = matches!(to, ColumnType::VarBit(_));
            let unbounded = cast_in(
                value,
                if varying {
                    ColumnType::VarBit(None)
                } else {
                    ColumnType::Bit(None)
                },
                style,
            )?;
            let Datum::BitString(bits) = &unbounded else {
                return Ok(unbounded);
            };
            if varying {
                bits.coerce_varbit(len, false).map(Datum::BitString)
            } else {
                bits.coerce_bit(len, false).map(Datum::BitString)
            }
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
            let Datum::Array(mut array) = cast_in(value, ColumnType::Array(unbounded), style)?
            else {
                return Ok(Datum::Null);
            };
            for element in &mut array.elems {
                *element = bounded_string(element, elem.column_type())?;
            }
            array.elem = elem;
            Ok(Datum::Array(array))
        }
        _ => cast_in(value, to, style),
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

/// [`cast`] with the session's `DateStyle` field order. That order decides how
/// the `text → date`/`timestamp`/`timestamptz` arms read an otherwise-ambiguous
/// all-numeric date literal (`01/02/03`). Every other arm ignores it.
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
        (Datum::Point(point), ColumnType::Point) => Ok(Datum::Point(*point)),
        (Datum::Path(path), ColumnType::Path) => Ok(Datum::Path(path.clone())),
        (Datum::Lseg(lseg), ColumnType::Lseg) => Ok(Datum::Lseg(*lseg)),
        (Datum::Line(line), ColumnType::Line) => Ok(Datum::Line(*line)),
        (Datum::Circle(circle), ColumnType::Circle) => Ok(Datum::Circle(*circle)),
        (Datum::Box(value), ColumnType::Box) => Ok(Datum::Box(*value)),
        (Datum::Polygon(polygon), ColumnType::Polygon) => Ok(Datum::Polygon(polygon.clone())),
        // The geometric conversions, each running the `pg_cast` function of the
        // target type's name. `polygon(path)` is the only one that can fail: an
        // open path has no polygon, which upstream reports as 22023 rather than
        // the NULL its neighbours return.
        (Datum::Point(point), ColumnType::Box) => {
            Ok(Datum::Box(crate::geometry::Box2::of_point(*point)))
        }
        (Datum::Lseg(lseg), ColumnType::Point) => Ok(Datum::Point(lseg.center())),
        (Datum::Path(path), ColumnType::Polygon) => path.to_polygon().map(Datum::Polygon),
        (Datum::Box(value), ColumnType::Point) => Ok(Datum::Point(value.center())),
        (Datum::Box(value), ColumnType::Lseg) => Ok(Datum::Lseg(value.diagonal())),
        (Datum::Box(value), ColumnType::Polygon) => Ok(Datum::Polygon(value.to_polygon())),
        (Datum::Box(value), ColumnType::Circle) => Ok(Datum::Circle(value.to_circle())),
        (Datum::Polygon(polygon), ColumnType::Point) => Ok(Datum::Point(polygon.to_point())),
        (Datum::Polygon(polygon), ColumnType::Path) => Ok(Datum::Path(polygon.to_path())),
        (Datum::Polygon(polygon), ColumnType::Box) => Ok(Datum::Box(polygon.to_box())),
        (Datum::Polygon(polygon), ColumnType::Circle) => Ok(Datum::Circle(polygon.to_circle())),
        (Datum::Circle(circle), ColumnType::Point) => Ok(Datum::Point(circle.to_point())),
        (Datum::Circle(circle), ColumnType::Box) => Ok(Datum::Box(circle.to_box())),
        // `polygon(circle)` is a SQL function reading `polygon(12, $1)`, so the
        // cast produces a twelve-vertex polygon whatever the radius.
        (Datum::Circle(circle), ColumnType::Polygon) => {
            circle.to_polygon(CIRCLE_CAST_VERTICES).map(Datum::Polygon)
        }
        // The network family. `inet → cidr` is `inet_to_cidr`, which zeroes
        // every bit to the right of the netmask; `cidr → inet` only re-labels.
        (Datum::Inet(value), ColumnType::Inet) => Ok(Datum::Inet(value.as_inet())),
        (Datum::Inet(value), ColumnType::Cidr) => Ok(Datum::Inet(value.to_cidr())),
        (Datum::MacAddr(value), ColumnType::MacAddr) => Ok(Datum::MacAddr(*value)),
        (Datum::MacAddr8(value), ColumnType::MacAddr8) => Ok(Datum::MacAddr8(*value)),
        (Datum::MacAddr(value), ColumnType::MacAddr8) => Ok(Datum::MacAddr8(value.to_macaddr8())),
        (Datum::MacAddr8(value), ColumnType::MacAddr) => value.to_macaddr().map(Datum::MacAddr),
        // The system identifier family's own conversions. `oid` is the only one
        // with any: `int2`/`int4` are binary coercions, so `(-1)::oid` is
        // 4294967295 and `4294967295::oid::int4` is -1, while `oid(bigint)` and
        // `int8(oid)` are real functions and the first range-checks.
        (Datum::Oid(value), ColumnType::Oid) => Ok(Datum::Oid(*value)),
        (Datum::Xid(value), ColumnType::Xid) => Ok(Datum::Xid(*value)),
        (Datum::Xid8(value), ColumnType::Xid8) => Ok(Datum::Xid8(*value)),
        (Datum::Cid(value), ColumnType::Cid) => Ok(Datum::Cid(*value)),
        (Datum::Tid(value), ColumnType::Tid) => Ok(Datum::Tid(*value)),
        (Datum::PgLsn(value), ColumnType::PgLsn) => Ok(Datum::PgLsn(*value)),
        (Datum::Int2(n), ColumnType::Oid) => Ok(Datum::Oid(i32::from(*n) as u32)),
        (Datum::Int4(n), ColumnType::Oid) => Ok(Datum::Oid(*n as u32)),
        (Datum::Int8(n), ColumnType::Oid) => {
            u32::try_from(*n)
                .map(Datum::Oid)
                .map_err(|_| TypeError::OutOfRange {
                    message: "OID out of range".to_string(),
                })
        }
        (Datum::Oid(value), Int4) => Ok(Datum::Int4(*value as i32)),
        (Datum::Oid(value), Int8) => Ok(Datum::Int8(i64::from(*value))),
        // `xid8toxid` keeps the low 32 bits — the epoch is what `xid8` adds.
        (Datum::Xid8(value), ColumnType::Xid) => Ok(Datum::Xid(*value as u32)),
        // `oidin` / `xidin` / `cidin` / `xid8in` / `tidin` / `pg_lsn_in`.
        (Datum::Text(s), ColumnType::Oid) => crate::sysid::uint32_in(s, "oid").map(Datum::Oid),
        (Datum::Text(s), ColumnType::Xid) => crate::sysid::uint32_in(s, "xid").map(Datum::Xid),
        (Datum::Text(s), ColumnType::Cid) => crate::sysid::uint32_in(s, "cid").map(Datum::Cid),
        (Datum::Text(s), ColumnType::Xid8) => crate::sysid::uint64_in(s, "xid8").map(Datum::Xid8),
        (Datum::Text(s), ColumnType::Tid) => crate::sysid::Tid::parse(s).map(Datum::Tid),
        (Datum::Text(s), ColumnType::PgLsn) => crate::sysid::lsn_in(s).map(Datum::PgLsn),
        // `pg_snapshot_in`, which `txid_snapshot_in` also is — so both cast
        // targets run the same grammar and report the same 22P02, naming
        // `pg_snapshot` even when `txid_snapshot` was written.
        (Datum::Text(s), ColumnType::PgSnapshot | ColumnType::TxidSnapshot) => s
            .parse::<crate::snapshot::PgSnapshot>()
            .map(|snapshot| Datum::PgSnapshot(Box::new(snapshot))),
        // Relabelling one snapshot type as the other is the identity: the two
        // hold one value, and no `pg_cast` entry converts between them anyway.
        (Datum::PgSnapshot(value), ColumnType::PgSnapshot | ColumnType::TxidSnapshot) => {
            Ok(Datum::PgSnapshot(value.clone()))
        }
        // `money`'s own conversions. `cash_numeric` divides by 100 and keeps
        // scale 2; `numeric_cash` / `int4_cash` / `int8_cash` multiply by 100
        // and report `bigint out of range` on overflow, because they delegate
        // to `numeric_int8` and `int8mul`.
        (Datum::Money(value), ColumnType::Money) => Ok(Datum::Money(*value)),
        (Datum::Money(value), Numeric(_)) => Ok(Datum::Numeric(crate::money::to_numeric(*value))),
        (Datum::Numeric(value), ColumnType::Money) => {
            crate::money::from_numeric(value).map(Datum::Money)
        }
        (Datum::Int4(n), ColumnType::Money) => crate::money::from_int4(*n).map(Datum::Money),
        (Datum::Int8(n), ColumnType::Money) => crate::money::from_int8(*n).map(Datum::Money),
        // `"char"`. The string directions run `charin`/`charout`, which the
        // three `→ text` arms below already reach through the value's own text
        // encoding: `char.c` notes that `char(text)` and `text(char)` differ
        // from the I/O pair only in *how* they reach the empty string, and both
        // pairs map it to `0x00` and back. `char → bpchar` is the one place
        // upstream diverges — `char_bpchar` copies the byte raw, so `\377`
        // becomes a `bpchar` no encoding can validate — and crabka keeps the
        // escaped form there rather than build an invalid string.
        (Datum::InternalChar(c), ColumnType::InternalChar) => Ok(Datum::InternalChar(*c)),
        (Datum::Text(s), ColumnType::InternalChar) => {
            Ok(Datum::InternalChar(crate::internal_char::parse(s)))
        }
        (Datum::InternalChar(c), Int4) => Ok(Datum::Int4(crate::internal_char::to_int4(*c))),
        (Datum::Int4(n), ColumnType::InternalChar) => {
            crate::internal_char::from_int4(*n).map(Datum::InternalChar)
        }
        // `bit()` / `varbit()` — the length coercions, which under an explicit
        // cast zero-pad or truncate rather than rejecting a mismatch. A target
        // with no modifier only relabels, which is `pg_cast`'s binary coercion
        // between the two types.
        (Datum::BitString(bits), ColumnType::Bit(len)) => bits
            .coerce_bit(len.unwrap_or(-1), true)
            .map(Datum::BitString),
        (Datum::BitString(bits), ColumnType::VarBit(len)) => bits
            .coerce_varbit(len.unwrap_or(-1), true)
            .map(Datum::BitString),
        // `bittoint4` / `bittoint8`: the bits read right-aligned as a two's
        // complement integer. `int2` has no such cast in PostgreSQL.
        (Datum::BitString(bits), Int4) => bits.to_int4().map(Datum::Int4),
        (Datum::BitString(bits), Int8) => bits.to_int8().map(Datum::Int8),
        // `bitfromint4` / `bitfromint8`: the low `n` bits, sign-extended.
        (Datum::Int4(n), ColumnType::Bit(len)) => Ok(Datum::BitString(
            crate::bitstring::BitString::from_int(i64::from(*n), len),
        )),
        (Datum::Int8(n), ColumnType::Bit(len)) => Ok(Datum::BitString(
            crate::bitstring::BitString::from_int(*n, len),
        )),
        (Datum::Text(s), Text) => Ok(Datum::Text(s.clone())),
        // The executor owns jsonpath parsing/canonicalization. Keeping only
        // identity here makes it impossible for a raw string to masquerade as
        // a validated jsonpath through a context-free type-layer call.
        (Datum::JsonPath(s), ColumnType::JsonPath) => Ok(Datum::JsonPath(s.clone())),
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
        // `json` identity keeps the text; the type carries no modifier that
        // could make a re-coercion do anything.
        (Datum::Json(text), ColumnType::Json) => Ok(Datum::Json(text.clone())),
        // `xml` the same, and for the same reason.
        (Datum::Xml(text), ColumnType::Xml) => Ok(Datum::Xml(text.clone())),
        // `xml → text` is `castmethod = 'b'`: the stored bytes, reinterpreted.
        // It must NOT go through the generic `(d, Text)` arm below, because
        // that runs the output function — and `xml_out` rewrites the XML
        // declaration, which a binary coercion cannot.
        (Datum::Xml(text), Text) => Ok(Datum::Text(text.clone())),
        (Datum::Xml(text), ColumnType::Varchar(n)) => {
            crate::string::apply_varchar_typmod(text, n, Coercion::Explicit).map(Datum::Text)
        }
        (Datum::Xml(text), ColumnType::Char(n)) => {
            crate::string::apply_char_typmod(text, n, Coercion::Explicit).map(Datum::Text)
        }
        // text → xml is `xml(text)`, i.e. `xml_in` under the session's
        // `xmloption`. The type layer has no session, so it validates under the
        // default, CONTENT; `XMLPARSE(DOCUMENT …)` is the spelling that reaches
        // the other grammar.
        (Datum::Text(s), ColumnType::Xml) => {
            crate::xml::validate(s, crate::xml::XmlOption::Content).map(|()| Datum::Xml(s.clone()))
        }
        // The two `pg_cast` entries, both `castmethod = 'i'`: each runs the
        // other type's input function over this one's output text.
        (Datum::Json(text), ColumnType::Jsonb) => crate::jsonb::parse(text).map(Datum::Jsonb),
        (Datum::Jsonb(j), ColumnType::Json) => Ok(Datum::Json(j.to_text())),
        (Datum::OidVector(vector), ColumnType::OidVector) => Ok(Datum::OidVector(vector.clone())),
        // Identity only when the elements already are int2s. There is no
        // oidvector -> int2vector cast upstream, so a vector of oids reaching
        // here has to fall through and be refused.
        (Datum::OidVector(vector), ColumnType::Int2Vector)
            if vector.elem == crate::ElemType::Int2 =>
        {
            Ok(Datum::OidVector(vector.clone()))
        }
        (Datum::TsVector(vector), ColumnType::TsVector) => Ok(Datum::TsVector(vector.clone())),
        (Datum::TsQuery(query), ColumnType::TsQuery) => Ok(Datum::TsQuery(query.clone())),
        // text → jsonb is `jsonb_in` (22P02 on bad JSON).
        (Datum::Text(s), ColumnType::Jsonb) => crate::jsonb::parse(s).map(Datum::Jsonb),
        // text → json is `json_in`, which validates and keeps every byte. The
        // whitespace and key order a `jsonb` cast would discard survive here.
        (Datum::Text(s), ColumnType::Json) => {
            crate::json::validate(s).map(|()| Datum::Json(s.clone()))
        }
        // text → array is `array_in`: split the literal, then run the element
        // type's input function over each element.
        (Datum::Text(s), ColumnType::Array(elem)) => {
            let raw = crate::array::parse_literal(s)?;
            let elems = raw
                .elements
                .into_iter()
                .map(|e| match e {
                    None => Ok(Datum::Null),
                    // The element input function reads the session's styles
                    // too: `'{97/02/10}'::date[]` is `array_in` calling
                    // `date_in`, which consults `DateStyle`'s field order.
                    Some(text) => cast_in(&Datum::Text(text), elem.column_type(), style),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(crate::datum::ArrayValue::with_dims(
                elem, elems, raw.dims,
            )))
        }
        // `int2vector` shares `oidvector`'s zero-based, space-separated form;
        // element failures report the element type, as PostgreSQL's
        // `int2vector_in` does.
        (Datum::Text(s), ColumnType::Int2Vector) => {
            let elems = s
                .split_whitespace()
                .map(text_to_i16)
                .collect::<Result<Vec<_>, _>>()?;
            let len = i32::try_from(elems.len()).unwrap_or(i32::MAX);
            Ok(Datum::OidVector(crate::datum::ArrayValue::with_dims(
                crate::ElemType::Int2,
                elems,
                vec![crate::ArrayDim::new(0, len)],
            )))
        }
        // `oidvectorin`, whose element reader is `oidin` — not `int4in`. The
        // two disagree on the base (`010` is 8, not 10), on the range (an oid
        // is unsigned, so `-1` and `4294967295` are the same legal value), on
        // where an element ends, and on the type their errors name. The `u32`
        // rides in an `Int4` because crabka has no `oid` element type; the bit
        // pattern is preserved, and both the text and the binary output read it
        // back unsigned.
        (Datum::Text(s), ColumnType::OidVector) => {
            let elems = crate::sysid::oidvector_in(s)?
                .into_iter()
                .map(|oid| Datum::Int4(oid.cast_signed()))
                .collect::<Vec<_>>();
            let len = i32::try_from(elems.len()).unwrap_or(i32::MAX);
            Ok(Datum::OidVector(crate::datum::ArrayValue::with_dims(
                crate::ElemType::Int4,
                elems,
                vec![crate::ArrayDim::new(0, len)],
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
                .map(|e| cast_in(e, elem.column_type(), style))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(crate::datum::ArrayValue::with_dims(
                elem,
                elems,
                a.dims.clone(),
            )))
        }
        (Datum::OidVector(a), ColumnType::Array(elem)) => {
            let elems = a
                .elems
                .iter()
                .map(|e| cast_in(e, elem.column_type(), style))
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
        // `inet`/`cidr` → `text` is `network_show`, NOT the output function:
        // it prints the whole host address and always appends the netmask, so
        // `'192.168.1.226'::inet::text` is `192.168.1.226/32`.
        (Datum::Inet(value), Text) => Ok(Datum::Text(value.show())),
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
        (Datum::Text(s), ColumnType::Bytea) => text_to_bytea(s).map(Datum::Bytea),
        (Datum::Text(s), ColumnType::Point) => crate::Point::parse(s).map(Datum::Point),
        (Datum::Text(s), ColumnType::Path) => crate::Path::parse(s).map(Datum::Path),
        (Datum::Text(s), ColumnType::Lseg) => crate::geometry::Lseg::parse(s).map(Datum::Lseg),
        (Datum::Text(s), ColumnType::Line) => crate::geometry::Line::parse(s).map(Datum::Line),
        (Datum::Text(s), ColumnType::Circle) => {
            crate::geometry::Circle::parse(s).map(Datum::Circle)
        }
        (Datum::Text(s), ColumnType::Box) => crate::geometry::Box2::parse(s).map(Datum::Box),
        (Datum::Text(s), ColumnType::Polygon) => {
            crate::geometry::Polygon::parse(s).map(Datum::Polygon)
        }
        // `regclass` → the oid family drops the name and keeps the oid, which is
        // what `regclass::oid`/`::int` yields in PostgreSQL.
        (Datum::Regclass(r), reg) if reg.is_reg() => Ok(Datum::Regclass(r.clone())),
        (Datum::Regclass(r), Int4) => Ok(Datum::Int4(r.oid)),
        (Datum::Regclass(r), Int8) => Ok(Datum::Int8(i64::from(r.oid))),
        // `reg* ↔ oid` are binary coercions, so the oid survives unchanged in
        // both directions and only the rendering differs.
        (Datum::Regclass(r), ColumnType::Oid) => Ok(Datum::Oid(r.oid as u32)),
        (Datum::Oid(value), reg) if reg.is_reg() => Ok(Datum::Regclass(
            crate::RegclassValue::unresolved(*value as i32),
        )),
        // → a `reg*` type. The pure cast has no catalog, so it can only produce
        // the unresolved rendering (`regclassout`'s bare-oid fallback); the
        // executor resolves the name before reaching here when a catalog is in
        // scope. An object NAME likewise needs the catalog — a non-numeric
        // string that falls through is 22P02, mirroring an unresolvable input.
        (Datum::Int4(n), reg) if reg.is_reg() => {
            Ok(Datum::Regclass(crate::RegclassValue::unresolved(*n)))
        }
        // A `bigint` reaches a `reg*` type the way PostgreSQL routes it: through
        // the implicit `oid(bigint)`, which range-checks against the UNSIGNED
        // 32-bit range. `4294967295::regclass` is therefore a valid oid, not
        // `integer out of range`.
        (Datum::Int8(n), reg) if reg.is_reg() => u32::try_from(*n)
            .map(|oid| Datum::Regclass(crate::RegclassValue::unresolved(oid as i32)))
            .map_err(|_| TypeError::OutOfRange {
                message: "OID out of range".to_string(),
            }),
        (Datum::Text(s), reg) if reg.is_reg() => s
            .trim()
            .parse::<i32>()
            .map(|n| Datum::Regclass(crate::RegclassValue::unresolved(n)))
            .map_err(|_| TypeError::InvalidText {
                type_name: reg.name(),
                value: s.clone(),
            }),
        // `cash_in`.
        (Datum::Text(s), ColumnType::Money) => crate::money::parse(s).map(Datum::Money),
        // `bit_in` / `varbit_in` with no length modifier, then the length
        // coercion — which is how PostgreSQL coerces an unknown literal, and
        // why `'1011'::bit(8)` pads while storing `'1011'` in a `bit(8)`
        // column does not.
        (Datum::Text(s), ColumnType::Bit(len)) => crate::bitstring::BitString::parse(s, false)?
            .coerce_bit(len.unwrap_or(-1), true)
            .map(Datum::BitString),
        (Datum::Text(s), ColumnType::VarBit(len)) => crate::bitstring::BitString::parse(s, true)?
            .coerce_varbit(len.unwrap_or(-1), true)
            .map(Datum::BitString),
        (Datum::Text(s), ColumnType::TsVector) => s.parse().map(Datum::TsVector),
        (Datum::Text(s), ColumnType::TsQuery) => s.parse().map(Datum::TsQuery),
        // `inet_in` / `cidr_in` / `macaddr_in` / `macaddr8_in`.
        (Datum::Text(s), ColumnType::Inet) => {
            crate::network::Inet::parse(s, false).map(Datum::Inet)
        }
        (Datum::Text(s), ColumnType::Cidr) => crate::network::Inet::parse(s, true).map(Datum::Inet),
        (Datum::Text(s), ColumnType::MacAddr) => {
            crate::network::MacAddr::parse(s).map(Datum::MacAddr)
        }
        (Datum::Text(s), ColumnType::MacAddr8) => {
            crate::network::MacAddr8::parse(s).map(Datum::MacAddr8)
        }
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
            Ok(Datum::Time(dt.time().into()))
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
            Ok(Datum::Time(tz.to_datetime(*ts).time().into()))
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
                time: tz.to_datetime(*ts).time().into(),
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

/// Wrap a value as a numeric `Datum`, and apply a `numeric(p,s)` modifier
/// (round to scale + precision overflow → 22003) when the target carries one.
/// Casts to and from the user-defined types, `Ok(None)` when neither side is
/// one and the built-in table should decide.
///
/// A domain unwraps to its base, because the value of a domain *is* a base
/// value. The executor checks the domain's own constraints, because it is the
/// only layer that can evaluate their `CHECK` expressions. A composite converts
/// from its text form (`record_in`), from another composite field by field, and
/// to text (`record_out`). An enum converts from and to text only.
fn cast_user_type(
    value: &Datum,
    to: ColumnType,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Option<Datum>, TypeError> {
    if let ColumnType::Multirange(multirange) = to {
        if let Datum::Multirange(existing) = value {
            return if existing.ty == multirange {
                Ok(Some(Datum::Multirange(existing.clone())))
            } else {
                Err(cannot_cast(value, to))
            };
        }
        if let Datum::Range(range) = value
            && range.ty == multirange.range
        {
            return crate::multirange::from_ranges(multirange, vec![range.clone()])
                .map(Datum::Multirange)
                .map(Some);
        }
        let Datum::Text(text) = value else {
            return Err(cannot_cast(value, to));
        };
        return crate::multirange::parse(text, multirange, style.time_zone)
            .map(Datum::Multirange)
            .map(Some);
    }
    if let ColumnType::Range(range) = to {
        if let Datum::Range(existing) = value {
            return if existing.ty == range {
                Ok(Some(Datum::Range(existing.clone())))
            } else {
                Err(cannot_cast(value, to))
            };
        }
        let Datum::Text(text) = value else {
            return Err(cannot_cast(value, to));
        };
        return crate::range::parse(text, range, style.time_zone)
            .map(Datum::Range)
            .map(Some);
    }
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
        (Datum::Range(_) | Datum::Multirange(_), _) if to.is_string() => {
            Ok(Some(string_result(text_of(value, style), to)?))
        }
        // Either operand is a user type and no rule above applies.
        (Datum::Record(_) | Datum::Enum(_) | Datum::Range(_) | Datum::Multirange(_), _)
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

/// A composite → composite coercion: field counts must agree, and this function
/// casts each field to the target attribute's type.
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
/// one of the type's labels.
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
/// *already* infinite passes through, as it does in PostgreSQL.
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

/// `float8 → int8` (PostgreSQL `dtoi8`/`ftoi8`): round half-to-even then
/// range-check; non-finite / out of range is 22003 `bigint out of range`.
///
/// The range test is **half-open**, exactly as `FLOAT8_FITS_IN_INT64` is:
/// `i64::MAX` has no `f64` image, so `i64::MAX as f64` rounds *up* to 2⁶³ and an
/// inclusive test would admit 2⁶³ and then saturate the `as i64` cast back down
/// to `i64::MAX` — turning an overflow into a plausible wrong answer.
fn i8_from_f64(f: f64) -> Result<Datum, TypeError> {
    let r = f.round_ties_even();
    let min = i64::MIN as f64;
    if r >= min && r < -min {
        Ok(Datum::Int8(r as i64))
    } else {
        Err(TypeError::out_of_range_for("bigint"))
    }
}

/// `text → bool`, which mirrors PostgreSQL `boolin`/`parse_bool_with_len`: case-
/// insensitive, leading/trailing whitespace trimmed, then a non-empty prefix of
/// `true`/`false`/`yes`/`no`/`on`/`off`, or the single chars `1`/`0`. A prefix
/// must identify one spelling unambiguously, so bare `o` is 22P02 while `of`
/// is accepted as `off`.
fn text_to_bool(s: &str) -> Result<Datum, TypeError> {
    let t = s.trim().to_ascii_lowercase();
    let v = match t.as_bytes().first() {
        Some(b't') if "true".starts_with(&t) => true,
        Some(b'f') if "false".starts_with(&t) => false,
        Some(b'y') if "yes".starts_with(&t) => true,
        Some(b'n') if "no".starts_with(&t) => false,
        Some(b'o') if t.len() > 1 && "on".starts_with(&t) => true,
        Some(b'o') if t.len() > 1 && "off".starts_with(&t) => false,
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

/// `text → int2` (PostgreSQL `int2in`). Out of range reports the *original*
/// string, spaces and all, exactly as `pg_strtoint16_safe` does.
fn text_to_i16(s: &str) -> Result<Datum, TypeError> {
    let value = parse_pg_integer(s, "smallint")?;
    i16::try_from(value)
        .map(Datum::Int2)
        .map_err(|_| TypeError::value_out_of_range(s, "smallint"))
}

fn text_to_i32(s: &str) -> Result<Datum, TypeError> {
    let value = parse_pg_integer(s, "integer")?;
    i32::try_from(value)
        .map(Datum::Int4)
        .map_err(|_| TypeError::value_out_of_range(s, "integer"))
}

fn text_to_i64(s: &str) -> Result<Datum, TypeError> {
    parse_pg_integer(s, "bigint").map(Datum::Int8)
}

/// PostgreSQL's `pg_strtoint{16,32,64}_safe` grammar: optional surrounding
/// whitespace, an optional sign, an optional `0x`/`0o`/`0b` base prefix (either
/// case), then digits of that base which `_` may separate. Every `_` must be
/// immediately followed by a digit, so `1__0`, `100_`, and `_100` are all bad
/// syntax while `1_0` and `0x_10` are values. Without a prefix the first
/// character after the sign must itself be a digit.
///
/// The magnitude accumulates *negatively* so that `-0x8000000000000000` reaches
/// [`i64::MIN`] rather than overflowing one step short of it. Bad syntax is
/// 22P02; a well-formed value too wide for the target is 22003, quoting the
/// original text.
fn parse_pg_integer(text: &str, type_name: &'static str) -> Result<i64, TypeError> {
    let invalid = || TypeError::InvalidText {
        type_name,
        value: text.to_string(),
    };
    let out_of_range = || TypeError::value_out_of_range(text, type_name);

    let bytes = text.trim().as_bytes();
    let mut index = 0;
    let negative = match bytes.first() {
        Some(b'-') => {
            index = 1;
            true
        }
        Some(b'+') => {
            index = 1;
            false
        }
        _ => false,
    };

    let mut radix = 10;
    if bytes.get(index) == Some(&b'0')
        && let Some(prefix) = bytes.get(index + 1)
    {
        radix = match prefix.to_ascii_lowercase() {
            b'x' => 16,
            b'o' => 8,
            b'b' => 2,
            _ => 10,
        };
        if radix != 10 {
            index += 2;
        }
    }
    // Only the prefixed forms may open with a separator: `0x_10` is a value but
    // `_100` is not.
    if radix == 10 && !bytes.get(index).is_some_and(u8::is_ascii_digit) {
        return Err(invalid());
    }

    let mut accumulated: i64 = 0;
    let mut digits = 0_u32;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'_' {
            let follows = bytes.get(index + 1).copied().ok_or_else(invalid)?;
            if char::from(follows).to_digit(radix).is_none() {
                return Err(invalid());
            }
            index += 1;
            continue;
        }
        let digit = char::from(byte).to_digit(radix).ok_or_else(invalid)?;
        accumulated = accumulated
            .checked_mul(i64::from(radix))
            .and_then(|scaled| scaled.checked_sub(i64::from(digit)))
            .ok_or_else(out_of_range)?;
        digits += 1;
        index += 1;
    }
    if digits == 0 {
        return Err(invalid());
    }

    if negative {
        Ok(accumulated)
    } else {
        accumulated.checked_neg().ok_or_else(out_of_range)
    }
}

/// `text → float8`, which matches PostgreSQL `float8in`: trimmed, accepts
/// decimal / exponent forms and the specials `Infinity`/`-Infinity`/`NaN`/`inf`
/// (case-insensitive). Bad syntax is 22P02; a *finite* literal that overflows to
/// infinity (e.g. `'1e400'`) is 22003. An explicit infinity spelling is the
/// value `Infinity`, not an error, which is why this cannot reuse
/// [`crate::ops::float_literal`], whose grammar has no infinity spelling.
fn text_to_f64(s: &str) -> Result<Datum, TypeError> {
    let t = s.trim();
    let Ok(parsed) = t.parse::<f64>() else {
        return Err(TypeError::InvalidText {
            type_name: "double precision",
            value: s.to_string(),
        });
    };
    // `strtod` reports overflow and underflow alike through `ERANGE`, so a
    // finite literal that reaches infinity and one that flushes a non-zero
    // magnitude to zero are equally out of range, quoting the trimmed text.
    let underflowed = parsed == 0.0 && has_nonzero_digit(t);
    if underflowed || (parsed.is_infinite() && !is_infinity_spelling(t)) {
        return Err(TypeError::float_text_out_of_range(t, "double precision"));
    }
    Ok(Datum::Float8(parsed))
}

/// `text → float4`, which matches PostgreSQL `float4in`: trimmed,
/// decimal/exponent forms plus the case-insensitive `Infinity`/`inf`/`NaN`
/// spellings. Bad syntax is 22P02; a finite literal that overflows to infinity
/// OR flushes a non-zero magnitude to zero is 22003. `strtof` reports both
/// through `ERANGE`, so `'1e39'` and `'1e-46'` are equally out of range while
/// the subnormal `'1e-45'` is a value. The out-of-range message quotes the
/// *trimmed* text, as PostgreSQL does.
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

fn text_to_bytea(s: &str) -> Result<Vec<u8>, TypeError> {
    let invalid = || TypeError::InvalidText {
        type_name: "bytea",
        value: s.to_string(),
    };
    if let Some(hex) = s.strip_prefix("\\x") {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        let bytes = hex.as_bytes();
        let mut out = Vec::with_capacity(bytes.len() / 2);
        let mut index = 0;
        while index < bytes.len() {
            while matches!(bytes[index], b' ' | b'\t' | b'\r' | b'\n') {
                index += 1;
                if index == bytes.len() {
                    return Ok(out);
                }
            }
            let Some(low) = bytes.get(index + 1) else {
                return Err(invalid());
            };
            let Some((high, low)) = digit(bytes[index]).zip(digit(*low)) else {
                return Err(invalid());
            };
            out.push(high * 16 + low);
            index += 2;
        }
        return Ok(out);
    }

    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            out.push(bytes[index]);
            index += 1;
        } else if bytes.get(index + 1) == Some(&b'\\') {
            out.push(b'\\');
            index += 2;
        } else if let Some(octal) = bytes.get(index + 1..index + 4) {
            if octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
                let value = u16::from(octal[0] - b'0') * 64
                    + u16::from(octal[1] - b'0') * 8
                    + u16::from(octal[2] - b'0');
                out.push(u8::try_from(value).map_err(|_| invalid())?);
                index += 4;
            } else {
                return Err(invalid());
            }
        } else {
            return Err(invalid());
        }
    }
    Ok(out)
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

    /// The session styles a `DateStyle` of `'ISO, <order>'` produces.
    fn ordered(
        tz: &jiff::tz::TimeZone,
        order: crate::datetime::DateOrder,
    ) -> crate::encoding::OutputStyle<'_> {
        crate::encoding::OutputStyle {
            date_order: order,
            ..crate::encoding::OutputStyle::with_zone(tz)
        }
    }

    /// The field order reaches every arm that runs a type's input function,
    /// including the ones that run it *inside* a container: `array_in` and
    /// `record_in` call the element/attribute input function, and PostgreSQL's
    /// read those GUCs exactly as a bare `date_in` does. Checked against
    /// PostgreSQL 18.4.
    #[test]
    fn the_date_order_reaches_element_and_attribute_input_functions() {
        use assert2::assert;

        use crate::datetime::DateOrder;
        let tz = utc();
        let date_array = ColumnType::Array(crate::ElemType::Date);
        let expected = cast_in(
            &Datum::Text("1997-02-10".into()),
            ColumnType::Date,
            ordered(&tz, DateOrder::Ymd),
        )
        .expect("unambiguous date");
        for (order, literal) in [
            (DateOrder::Ymd, "97/02/10"),
            (DateOrder::Dmy, "10/02/97"),
            (DateOrder::Mdy, "02/10/97"),
        ] {
            let style = ordered(&tz, order);
            let parsed = cast_in(&Datum::Text(literal.into()), ColumnType::Date, style)
                .unwrap_or_else(|error| panic!("{order:?} {literal}: {error:?}"));
            assert!(parsed == expected, "{order:?} bare");

            let array = cast_in(&Datum::Text(format!("{{{literal}}}")), date_array, style)
                .unwrap_or_else(|error| panic!("{order:?} {{{literal}}}: {error:?}"));
            let Datum::Array(array) = array else {
                panic!("{order:?}: expected an array");
            };
            assert!(
                array.elems == vec![expected.clone()],
                "{order:?} array element"
            );

            // An assignment reads it too — the store path is not a different
            // parser from the one a written cast uses.
            let assigned = cast_assign_in(&Datum::Text(literal.into()), ColumnType::Date, style)
                .unwrap_or_else(|error| panic!("{order:?} assign {literal}: {error:?}"));
            assert!(assigned == expected, "{order:?} assignment");
        }
        // The order is decisive, not advisory: `31/01/97` is a date under DMY
        // and out of range under MDY, through the same three arms.
        for build in [ColumnType::Date, date_array] {
            let literal = |text: &str| {
                if matches!(build, ColumnType::Array(_)) {
                    format!("{{{text}}}")
                } else {
                    text.to_string()
                }
            };
            assert!(
                cast_in(
                    &Datum::Text(literal("31/01/97")),
                    build,
                    ordered(&tz, DateOrder::Dmy)
                )
                .is_ok()
            );
            assert!(
                cast_in(
                    &Datum::Text(literal("31/01/97")),
                    build,
                    ordered(&tz, DateOrder::Mdy)
                )
                .is_err()
            );
        }
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
    fn bytea_input_accepts_plain_hex_and_escape_forms() {
        let cast = |value: &str| {
            super::cast(&Datum::Text(value.into()), ColumnType::Bytea, &utc()).expect("valid bytea")
        };
        assert_eq!(cast("ABC"), Datum::Bytea(b"ABC".to_vec()));
        assert_eq!(cast("\\x414243"), Datum::Bytea(b"ABC".to_vec()));
        assert_eq!(cast("\\x 41 42 43 "), Datum::Bytea(b"ABC".to_vec()));
        assert!(super::cast(&Datum::Text("\\x4 1".into()), ColumnType::Bytea, &utc()).is_err());
        assert!(super::cast(&Datum::Text("\\x\x0b41".into()), ColumnType::Bytea, &utc()).is_err());
        assert_eq!(cast("A\\\\B\\103"), Datum::Bytea(b"A\\BC".to_vec()));
        assert!(super::cast(&Datum::Text("\\400".into()), ColumnType::Bytea, &utc()).is_err());
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
            Err(TypeError::StringDataRightTruncation { .. })
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
            Err(TypeError::StringDataRightTruncation { .. })
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
        assert!(
            cast(&Datum::Text("[]".into()), jsonb, &tz).expect("empty")
                == Datum::Jsonb(JsonbValue::Array(vec![]))
        );
    }

    #[test]
    fn json_casts_keep_the_input_text_and_jsonb_casts_normalise_it() {
        use assert2::assert;

        let tz = utc();
        let json = ColumnType::Json;
        let raw = r#"{"b":1,   "a":2,  "b":3}"#;
        // `json` is its own type, not a spelling of `jsonb`.
        assert!(ColumnType::from_sql_name("json") == Some(ColumnType::Json));
        assert!(ColumnType::Json.oid() == 114);
        assert!(ColumnType::Array(crate::ElemType::Json).oid() == 199);

        // text → json validates and changes nothing; text → jsonb decomposes.
        let value = cast(&Datum::Text(raw.into()), json, &tz).expect("text -> json");
        assert!(value == Datum::Json(raw.to_string()));
        assert!(
            cast(&value, ColumnType::Text, &tz).expect("json -> text") == Datum::Text(raw.into())
        );
        assert!(cast(&value, json, &tz).expect("identity") == value);

        // The two pg_cast entries: json → jsonb normalises, jsonb → json keeps
        // whatever the jsonb value had left.
        let as_jsonb = cast(&value, ColumnType::Jsonb, &tz).expect("json -> jsonb");
        assert!(
            cast(&as_jsonb, ColumnType::Text, &tz).expect("text")
                == Datum::Text(r#"{"a": 2, "b": 3}"#.into())
        );
        assert!(
            cast(&as_jsonb, json, &tz).expect("jsonb -> json")
                == Datum::Json(r#"{"a": 2, "b": 3}"#.to_string())
        );

        // Bad JSON is 22P02 with PostgreSQL's DETAIL, and NULL stays NULL.
        let err = cast(&Datum::Text("{oops".into()), json, &tz).expect_err("bad json");
        assert!(err.sqlstate() == "22P02");
        assert!(err.detail().as_deref() == Some("Token \"oops\" is invalid."));
        assert!(cast(&Datum::Null, json, &tz).expect("null") == Datum::Null);

        // PostgreSQL has no json ↔ number/bool cast, in either direction.
        for other in [ColumnType::Int4, ColumnType::Bool, ColumnType::Float8] {
            assert!(!cast_allowed(json, other), "{other:?}");
            assert!(!cast_allowed(other, json), "{other:?}");
        }
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
    fn range_array_casts_use_the_range_element_input_function() {
        let tz = utc();
        let range = match ColumnType::builtin_range(crate::oids::NUMRANGE).expect("numrange") {
            ColumnType::Range(range) => range,
            _ => unreachable!(),
        };
        let ty = ColumnType::Array(crate::ElemType::Range(range));
        let value = cast(
            &Datum::Text(r#"{"[1.1,1.2)","[12.3,155.5)"}"#.into()),
            ty,
            &tz,
        )
        .expect("numrange[] input");
        assert_eq!(
            crate::encoding::encode_text(&value, &tz),
            br#"{"[1.1,1.2)","[12.3,155.5)"}"#
        );

        let mut encoded = Vec::new();
        crate::ElemType::Range(range).write_code(&mut encoded);
        assert_eq!(
            crate::ElemType::read_code(&mut encoded.as_slice()),
            Some(crate::ElemType::Range(range))
        );
    }

    #[test]
    fn multirange_array_casts_use_the_multirange_element_input_function() {
        let tz = utc();
        let multirange = match ColumnType::builtin_multirange(crate::oids::NUMMULTIRANGE)
            .expect("nummultirange")
        {
            ColumnType::Multirange(multirange) => multirange,
            _ => unreachable!(),
        };
        let elem = crate::ElemType::Multirange(multirange);
        let value = cast(
            &Datum::Text(r#"{"{[1.1,1.2)}","{[12.3,155.5)}"}"#.into()),
            ColumnType::Array(elem),
            &tz,
        )
        .expect("nummultirange[] input");
        assert_eq!(
            crate::encoding::encode_text(&value, &tz),
            br#"{"{[1.1,1.2)}","{[12.3,155.5)}"}"#
        );
        assert_eq!(elem.array_oid(), crate::oids::NUMMULTIRANGEARRAY);
        assert_eq!(
            crate::ElemType::from_array_oid(crate::oids::NUMMULTIRANGEARRAY),
            Some(elem)
        );

        let mut encoded = Vec::new();
        elem.write_code(&mut encoded);
        assert_eq!(
            crate::ElemType::read_code(&mut encoded.as_slice()),
            Some(elem)
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
            // float8 names itself the same way, for overflow and underflow.
            (
                Datum::Text(" 1e999 ".into()),
                ColumnType::Float8,
                "22003",
                "\"1e999\" is out of range for type double precision",
            ),
            (
                Datum::Text("1e-400".into()),
                ColumnType::Float8,
                "22003",
                "\"1e-400\" is out of range for type double precision",
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

    /// PostgreSQL's integer input accepts `0x`/`0o`/`0b` bases and `_` digit
    /// separators, and reports a too-wide but well-formed value as 22003
    /// quoting the original text — including for `int4`/`int8`, which used to
    /// fall back to the bare arithmetic `... out of range` message.
    #[test]
    fn integer_input_accepts_postgres_bases_and_separators() {
        use ColumnType::{Int2, Int4, Int8};
        use assert2::assert;
        let tz = utc();
        let values: &[(&str, ColumnType, Datum)] = &[
            ("0x42F", Int4, Datum::Int4(1071)),
            ("0X10", Int4, Datum::Int4(16)),
            ("0o17", Int4, Datum::Int4(15)),
            ("0b1010", Int4, Datum::Int4(10)),
            ("1_000_000", Int4, Datum::Int4(1_000_000)),
            // A separator may open the digits only after a base prefix.
            ("0x_10", Int4, Datum::Int4(16)),
            (" 0x10 ", Int4, Datum::Int4(16)),
            ("-0x10", Int4, Datum::Int4(-16)),
            ("0b100101", Int2, Datum::Int2(37)),
            ("0x7fff", Int2, Datum::Int2(32767)),
            // The magnitude accumulates negatively, so MIN is reachable.
            ("-0x8000", Int2, Datum::Int2(i16::MIN)),
            ("-0x80000000", Int4, Datum::Int4(i32::MIN)),
            ("-0x8000000000000000", Int8, Datum::Int8(i64::MIN)),
            ("0b1_1", Int8, Datum::Int8(3)),
        ];
        for (text, target, expected) in values {
            let actual = cast(&Datum::Text((*text).into()), *target, &tz);
            assert!(actual.as_ref() == Ok(expected), "{text:?} -> {target:?}");
        }

        let errors: &[(&str, ColumnType, &str, &str)] = &[
            (
                "0x8000000000000000",
                Int8,
                "22003",
                "value \"0x8000000000000000\" is out of range for type bigint",
            ),
            (
                "1000000000000",
                Int4,
                "22003",
                "value \"1000000000000\" is out of range for type integer",
            ),
            (
                "0b1000000000000000",
                Int2,
                "22003",
                "value \"0b1000000000000000\" is out of range for type smallint",
            ),
            // Every `_` must be followed by a digit of the same base.
            (
                "1__0",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"1__0\"",
            ),
            (
                "100_",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"100_\"",
            ),
            (
                "_100",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"_100\"",
            ),
            (
                "0x__1",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"0x__1\"",
            ),
            (
                "0x",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"0x\"",
            ),
            // Digits must belong to the declared base.
            (
                "0b12",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"0b12\"",
            ),
            (
                "0o8",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"0o8\"",
            ),
            (
                "00x10",
                Int4,
                "22P02",
                "invalid input syntax for type integer: \"00x10\"",
            ),
            (
                "    ",
                Int2,
                "22P02",
                "invalid input syntax for type smallint: \"    \"",
            ),
        ];
        for (text, target, sqlstate, message) in errors {
            let err = cast(&Datum::Text((*text).into()), *target, &tz)
                .expect_err("bad syntax or out of range");
            assert!(err.sqlstate() == *sqlstate, "{text:?} -> {target:?}");
            assert!(err.to_string() == *message, "{text:?} -> {target:?}");
        }
    }

    /// `int2vector` shares `oidvector`'s zero-based, space-separated form, and
    /// reports a bad element the way the element type's input function does.
    #[test]
    fn int2vector_parses_the_space_separated_zero_based_form() {
        use assert2::assert;
        let tz = utc();
        let parsed =
            cast(&Datum::Text(" 1 3  5 ".into()), ColumnType::Int2Vector, &tz).expect("int2vector");
        let Datum::OidVector(vector) = parsed else {
            panic!("int2vector is the shared zero-based vector datum")
        };
        assert!(vector.elem == crate::ElemType::Int2);
        assert!(vector.dims == vec![crate::ArrayDim::new(0, 3)]);
        assert!(vector.elems == vec![Datum::Int2(1), Datum::Int2(3), Datum::Int2(5)]);

        for (text, sqlstate, message) in [
            (
                "1 asdf",
                "22P02",
                "invalid input syntax for type smallint: \"asdf\"",
            ),
            (
                "50000",
                "22003",
                "value \"50000\" is out of range for type smallint",
            ),
        ] {
            let err = cast(&Datum::Text(text.into()), ColumnType::Int2Vector, &tz)
                .expect_err("bad element");
            assert!(err.sqlstate() == sqlstate, "{text}");
            assert!(err.to_string() == message, "{text}: {err}");
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
        // `int8` reports its own width, not `integer out of range`.
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let error = cast(&Datum::Float8(value), ColumnType::Int8, &tz).expect_err("i8 range");
            assert!(error.sqlstate() == "22003");
            assert!(error.to_string() == "bigint out of range");
        }
        // 2⁶³ has an exact `f64` (and `f32`) image but no `i64` one, so the
        // range test has to be half-open: `9223372036854775807::float4` rounds
        // *up* to 2⁶³ and PostgreSQL rejects it rather than saturating.
        for value in [9.223_372_036_854_776e18_f64, -9.223_38e18] {
            let error = cast(&Datum::Float8(value), ColumnType::Int8, &tz).expect_err("i8 range");
            assert!(error.to_string() == "bigint out of range");
        }
        let error = cast(&Datum::Float4(9.223_372e18), ColumnType::Int8, &tz).expect_err("f4 i8");
        assert!(error.to_string() == "bigint out of range");
        // The largest `f64` strictly below 2⁶³ still converts.
        assert!(
            cast(
                &Datum::Float8(9.223_372_036_854_775e18),
                ColumnType::Int8,
                &tz
            )
            .expect("in range")
                == Datum::Int8(9_223_372_036_854_774_784)
        );
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
        // `o` is ambiguous between `on` and `off`; `of` identifies only `off`.
        assert!(matches!(
            cast(&Datum::Text("o".into()), ColumnType::Bool, &tz),
            Err(TypeError::InvalidText { .. })
        ));
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
        // `0x10` is a hexadecimal *value*; see
        // `integer_input_accepts_postgres_bases_and_separators`.
        for s in ["1.5", "abc", "", "  ", "-", "1e3"] {
            assert!(
                matches!(
                    cast(&Datum::Text(s.into()), ColumnType::Int4, &tz),
                    Err(TypeError::InvalidText { .. })
                ),
                "{s:?} should be 22P02"
            );
        }
        // Well-formed but out of range → 22003 (NOT 22P02), quoting the input
        // the way PostgreSQL's integer input functions do.
        for (text, target, message) in [
            (
                "99999999999",
                ColumnType::Int4,
                "value \"99999999999\" is out of range for type integer",
            ),
            (
                "99999999999999999999",
                ColumnType::Int8,
                "value \"99999999999999999999\" is out of range for type bigint",
            ),
        ] {
            let err = cast(&Datum::Text(text.into()), target, &tz).expect_err("out of range");
            assert!(err.sqlstate() == "22003", "{text:?}");
            assert!(err.to_string() == message, "{text:?}: {err}");
        }
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
        // A finite literal that overflows to ∞ is 22003, NOT the value Infinity,
        // and it names the type the way PostgreSQL's `float8in` does.
        let overflow = cast(&Datum::Text("1e400".into()), ColumnType::Float8, &tz)
            .expect_err("1e400 overflows");
        assert_eq!(overflow.sqlstate(), "22003");
        assert_eq!(
            overflow.to_string(),
            "\"1e400\" is out of range for type double precision"
        );
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

    // ---- the geometric family ----

    /// The seven geometric types, in `pg_type.oid` order.
    const GEOMETRIC: [ColumnType; 7] = [
        ColumnType::Point,
        ColumnType::Lseg,
        ColumnType::Path,
        ColumnType::Box,
        ColumnType::Polygon,
        ColumnType::Line,
        ColumnType::Circle,
    ];

    /// Every geometric row of `pg_cast` on PostgreSQL 18.4:
    ///
    /// ```sql
    /// SELECT castsource::regtype, casttarget::regtype, castcontext
    ///   FROM pg_cast
    ///  WHERE castsource::regtype::text
    ///        IN ('point','box','circle','line','lseg','path','polygon')
    ///     OR casttarget::regtype::text IN (…);
    /// ```
    ///
    /// Fourteen rows and no more. `line` appears in none of them.
    const GEOMETRIC_PG_CAST: [(ColumnType, ColumnType, bool); 14] = [
        (ColumnType::Point, ColumnType::Box, true),
        (ColumnType::Lseg, ColumnType::Point, false),
        (ColumnType::Path, ColumnType::Polygon, true),
        (ColumnType::Box, ColumnType::Point, false),
        (ColumnType::Box, ColumnType::Lseg, false),
        (ColumnType::Box, ColumnType::Polygon, true),
        (ColumnType::Box, ColumnType::Circle, false),
        (ColumnType::Polygon, ColumnType::Point, false),
        (ColumnType::Polygon, ColumnType::Path, true),
        (ColumnType::Polygon, ColumnType::Box, false),
        (ColumnType::Polygon, ColumnType::Circle, false),
        (ColumnType::Circle, ColumnType::Point, false),
        (ColumnType::Circle, ColumnType::Box, false),
        (ColumnType::Circle, ColumnType::Polygon, false),
    ];

    /// The static matrix must contain exactly `pg_cast`'s geometric rows: every
    /// declared pair allowed, every one of the other 35 ordered pairs refused.
    /// The absences carry as much weight as the presences — `point → polygon`
    /// and anything touching `line` are 42846 in PostgreSQL.
    #[test]
    fn geometric_cast_matrix_is_exactly_the_declared_pg_cast_rows() {
        use assert2::assert;
        for from in GEOMETRIC {
            for to in GEOMETRIC {
                let declared = GEOMETRIC_PG_CAST
                    .iter()
                    .any(|(source, target, _)| *source == from && *target == to);
                // Identity is always allowed and needs no `pg_cast` row.
                let expected = declared || from == to;
                assert!(
                    cast_allowed(from, to) == expected,
                    "explicit {from:?} -> {to:?}"
                );
            }
        }
    }

    /// Only the four rows PostgreSQL marks `castcontext = 'a'` convert without
    /// a written-out cast; the other ten are `'e'`. Storing a `circle` in a
    /// `polygon` column therefore still needs `::polygon`.
    #[test]
    fn geometric_assignment_matrix_is_exactly_the_assignment_context_rows() {
        use assert2::assert;
        for from in GEOMETRIC {
            for to in GEOMETRIC {
                let expected = from == to
                    || GEOMETRIC_PG_CAST
                        .iter()
                        .any(|(source, target, assignment)| {
                            *source == from && *target == to && *assignment
                        });
                assert!(
                    assignment_cast_allowed(from, to) == expected,
                    "assignment {from:?} -> {to:?}"
                );
                // The subset invariant the whole matrix rests on.
                assert!(
                    !assignment_cast_allowed(from, to) || cast_allowed(from, to),
                    "assignment outruns explicit for {from:?} -> {to:?}"
                );
            }
        }
    }

    /// Every expectation is `SELECT <source> '<literal>'::<target>::text` on
    /// PostgreSQL 18.4 at the default `extra_float_digits`.
    #[test]
    fn geometric_conversions_match_the_pg_cast_functions() {
        use assert2::assert;
        let tz = utc();
        let cases: &[(ColumnType, &str, ColumnType, &str)] = &[
            // `box(point)` — the degenerate box at the point.
            (ColumnType::Point, "(1,2)", ColumnType::Box, "(1,2),(1,2)"),
            // `point(lseg)` — the midpoint.
            (
                ColumnType::Lseg,
                "[(1,2),(3,4)]",
                ColumnType::Point,
                "(2,3)",
            ),
            // `polygon(path)` — the same vertices, for a CLOSED path.
            (
                ColumnType::Path,
                "((0,0),(1,0),(1,1))",
                ColumnType::Polygon,
                "((0,0),(1,0),(1,1))",
            ),
            (ColumnType::Box, "(1,2),(3,4)", ColumnType::Point, "(2,3)"),
            // `lseg(box)` — the positive-slope diagonal, high corner first.
            (
                ColumnType::Box,
                "(1,2),(3,4)",
                ColumnType::Lseg,
                "[(3,4),(1,2)]",
            ),
            // `polygon(box)` — the four corners anticlockwise from the low one.
            (
                ColumnType::Box,
                "(1,2),(3,4)",
                ColumnType::Polygon,
                "((1,2),(1,4),(3,4),(3,2))",
            ),
            // `circle(box)` circumscribes; `box(circle)` inscribes, so the two
            // are not inverses.
            (
                ColumnType::Box,
                "(1,2),(3,4)",
                ColumnType::Circle,
                "<(2,3),1.4142135623730951>",
            ),
            (
                ColumnType::Circle,
                "<(1,2),3>",
                ColumnType::Box,
                "(3.1213203435596424,4.121320343559642),(-1.1213203435596424,-0.12132034355964239)",
            ),
            (ColumnType::Circle, "<(1,2),3>", ColumnType::Point, "(1,2)"),
            // `point(polygon)` is the mean of the VERTICES, not the centre of
            // the bounding box — the two differ for this triangle.
            (
                ColumnType::Polygon,
                "((0,0),(2,0),(2,2))",
                ColumnType::Point,
                "(1.3333333333333333,0.6666666666666666)",
            ),
            (
                ColumnType::Polygon,
                "((0,0),(2,0),(2,2))",
                ColumnType::Path,
                "((0,0),(2,0),(2,2))",
            ),
            (
                ColumnType::Polygon,
                "((0,0),(2,0),(2,2))",
                ColumnType::Box,
                "(2,2),(0,0)",
            ),
            (
                ColumnType::Polygon,
                "((0,0),(2,0),(2,2))",
                ColumnType::Circle,
                "<(1.3333333333333333,0.6666666666666666),1.308077670527261>",
            ),
        ];
        for (source, literal, target, expected) in cases {
            let value = cast(&Datum::Text((*literal).into()), *source, &tz)
                .unwrap_or_else(|_| panic!("{literal} is a {source:?}"));
            let converted =
                cast(&value, *target, &tz).unwrap_or_else(|_| panic!("{source:?} -> {target:?}"));
            assert!(
                converted.column_type() == Some(*target),
                "{source:?} -> {target:?}"
            );
            let text = cast(&converted, ColumnType::Text, &tz).expect("render");
            assert!(
                text == Datum::Text((*expected).to_string()),
                "{source:?} '{literal}'::{target:?}"
            );
        }
    }

    /// `polygon(circle)` is `select pg_catalog.polygon(12, $1)` in `pg_proc`, so
    /// the cast yields twelve vertices whatever the radius. The rendering is
    /// `SELECT circle '<(0,0),2>'::polygon::text` on PostgreSQL 18.4.
    #[test]
    fn circle_to_polygon_produces_twelve_vertices() {
        use assert2::assert;
        let tz = utc();
        let circle = cast(&Datum::Text("<(0,0),2>".into()), ColumnType::Circle, &tz).expect("c");
        let Datum::Polygon(polygon) = cast(&circle, ColumnType::Polygon, &tz).expect("poly") else {
            panic!("circle::polygon is a polygon");
        };
        assert!(polygon.npoints() == 12);
        // Compared as geometry rather than as rendered text. `circle_poly` walks
        // the circle with `cos`/`sin`, and those differ by an ULP between
        // platforms' libm — macOS renders one vertex `0.9999999999999996` where
        // Linux renders `…97`. Pinning the string made this test assert which
        // libm built it. The invariant that matters is the shape: twelve
        // vertices on the circle, evenly spaced, from angle pi downward.
        for (i, point) in polygon.points.iter().enumerate() {
            #[expect(
                clippy::cast_precision_loss,
                reason = "twelve vertices; the index is exact in f64"
            )]
            let angle = std::f64::consts::PI - (i as f64) * std::f64::consts::TAU / 12.0;
            let (want_x, want_y) = (2.0 * angle.cos(), 2.0 * angle.sin());
            assert!(
                (point.x - want_x).abs() < 1e-12 && (point.y - want_y).abs() < 1e-12,
                "vertex {i}: {point:?} is not on the circle at angle {angle}"
            );
            assert!(
                (point.x.hypot(point.y) - 2.0).abs() < 1e-12,
                "vertex {i} radius"
            );
        }

        // A zero-radius circle has no polygon: `circle_poly` reports 0A000, not
        // a twelve-fold repetition of the centre.
        let degenerate =
            cast(&Datum::Text("<(0,0),0>".into()), ColumnType::Circle, &tz).expect("c");
        let error = cast(&degenerate, ColumnType::Polygon, &tz).expect_err("radius zero");
        assert!(error.sqlstate() == "0A000");
        assert!(
            error
                .to_string()
                .contains("cannot convert circle with radius zero to polygon")
        );
    }

    /// `path_poly` is the one geometric conversion that can fail: an open path
    /// has no polygon. Upstream reports 22023 rather than the NULL its
    /// neighbouring conversions return.
    #[test]
    fn open_path_to_polygon_is_22023() {
        use assert2::assert;
        let tz = utc();
        let open = cast(&Datum::Text("[(0,0),(1,1)]".into()), ColumnType::Path, &tz).expect("path");
        let error = cast(&open, ColumnType::Polygon, &tz).expect_err("open path");
        assert!(error.sqlstate() == "22023");
        assert!(
            error
                .to_string()
                .contains("open path cannot be converted to polygon")
        );
    }

    /// `polygon` reaches the string family through its input and output
    /// functions, like every other geometric type, and a bad literal is 22P02.
    #[test]
    fn polygon_converts_with_the_string_family_only_through_its_io_functions() {
        use assert2::assert;
        let tz = utc();
        assert!(cast_allowed(ColumnType::Text, ColumnType::Polygon));
        assert!(cast_allowed(ColumnType::Polygon, ColumnType::Text));
        // An I/O-conversion cast is explicit-only (PostgreSQL 8.3 onward).
        assert!(!assignment_cast_allowed(
            ColumnType::Text,
            ColumnType::Polygon
        ));
        assert!(!assignment_cast_allowed(
            ColumnType::Polygon,
            ColumnType::Text
        ));
        let value = cast(
            &Datum::Text("(0,0),(1,0),(1,1)".into()),
            ColumnType::Polygon,
            &tz,
        )
        .expect("poly_in");
        // Identity keeps the value.
        assert!(cast(&value, ColumnType::Polygon, &tz).expect("identity") == value);
        assert!(
            cast(&value, ColumnType::Text, &tz).expect("poly_out")
                == Datum::Text("((0,0),(1,0),(1,1))".into())
        );
        // `varchar`/`char` go through the same output function, then the typmod.
        assert!(
            cast(&value, ColumnType::Varchar(Some(5)), &tz).expect("varchar")
                == Datum::Text("((0,0".into())
        );
        let bad = cast(&Datum::Text("((0,0),(1".into()), ColumnType::Polygon, &tz)
            .expect_err("malformed");
        assert!(bad.sqlstate() == "22P02");
        // Nothing outside the geometric and string families.
        assert!(!cast_allowed(ColumnType::Polygon, ColumnType::Int4));
        assert!(!cast_allowed(ColumnType::Int4, ColumnType::Polygon));
        assert!(!cast_allowed(ColumnType::Polygon, ColumnType::Jsonb));
        let refused = cast(&value, ColumnType::Int4, &tz).expect_err("no polygon -> int4");
        assert!(refused.sqlstate() == "42846");
    }
}
