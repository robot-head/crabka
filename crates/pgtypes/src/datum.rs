//! The runtime value type and the SQL column types of the SP2 slice.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible datum API kept structurally close to donor"
)]

use bigdecimal::BigDecimal;

use crate::numeric::Typmod;

/// PostgreSQL type OIDs (from pg_type.dat) for the slice's types.
pub mod oids {
    pub const BOOL: u32 = 16;
    /// SP40: `bytea` — variable-length binary string.
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    /// PostgreSQL `smallint`; Bind parameters widen into the existing `Int4` datum.
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    /// PostgreSQL `oid` — object identifier, an unsigned 4-byte integer.
    ///
    /// Drivers send this OID for typeinfo-query parameters (e.g. tokio-postgres
    /// declares `WHERE t.oid = $1` with OID). Values live in the existing `Int4`
    /// datum — the same representation the catalog's oid-valued columns use.
    pub const OID: u32 = 26;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    /// PostgreSQL `real`; Bind parameters widen into the existing `Float8` datum.
    pub const FLOAT4: u32 = 700;
    /// SP30: `double precision` (IEEE-754 f64).
    pub const FLOAT8: u32 = 701;
    /// SP32: arbitrary-precision `numeric`/`decimal`.
    pub const NUMERIC: u32 = 1700;
    /// SP37: `date` — days since 2000-01-01, stored as i32.
    pub const DATE: u32 = 1082;
    /// SP37: `time without time zone` — microseconds since midnight, stored as i64.
    pub const TIME: u32 = 1083;
    /// SP37: `timestamp without time zone` — microseconds since 2000-01-01 00:00:00.
    pub const TIMESTAMP: u32 = 1114;
    /// SP37: `timestamp with time zone` — microseconds since Unix epoch (UTC), stored as i64.
    pub const TIMESTAMPTZ: u32 = 1184;
    /// SP37: `interval` — months (i32) + days (i32) + microseconds (i64), stored as 16 bytes.
    pub const INTERVAL: u32 = 1186;
    /// PostgreSQL `uuid` — 128-bit universally unique identifier.
    pub const UUID: u32 = 2950;
}

/// A SQL column type. SP30 added `Float8`; SP32 added `Numeric` (which carries an
/// optional `numeric(precision, scale)` modifier for column definitions / casts —
/// `None` is unconstrained `numeric`). SP37 adds five date/time types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int4,
    Int8,
    Text,
    Varchar(Option<u16>),
    Char(Option<u16>),
    /// SP30: PostgreSQL `double precision` (an IEEE-754 `f64`).
    Float8,
    /// SP32: PostgreSQL `numeric`/`decimal`. The `Typmod` (precision, scale) is
    /// significant only when storing/casting; OID/name/typlen ignore it.
    Numeric(Option<Typmod>),
    /// SP37: PostgreSQL `date` (OID 1082) — a calendar date with no time-of-day.
    Date,
    /// SP37: PostgreSQL `time without time zone` (OID 1083).
    Time,
    /// SP37: PostgreSQL `timestamp without time zone` (OID 1114).
    Timestamp,
    /// SP37: PostgreSQL `timestamp with time zone` (OID 1184) — stored as UTC.
    Timestamptz,
    /// SP37: PostgreSQL `interval` (OID 1186) — months + days + microseconds.
    Interval,
    /// SP40: PostgreSQL `bytea` (OID 17) — variable-length binary string.
    Bytea,
    /// PostgreSQL `uuid` (OID 2950) — 128-bit identifier.
    Uuid,
}

impl ColumnType {
    /// Resolve a bare SQL type name (no modifier). `numeric`/`decimal` resolve to
    /// the unconstrained form; the parser layers the `(p, s)` modifier on top.
    pub fn from_sql_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "int4" | "integer" | "int" => Some(ColumnType::Int4),
            "int8" | "bigint" => Some(ColumnType::Int8),
            // `oid` (object identifier, OID 26) is a pragmatic alias for `int4`:
            // the catalog's oid-valued columns (pg_type.oid, pg_namespace.oid,
            // pg_type.typnamespace, …) are Int4, so `NULL::oid` and
            // `CAST(x AS oid)` resolve consistently with them. RowDescription
            // consequently reports int4 (23), not oid (26), for such expressions.
            "oid" => Some(ColumnType::Int4),
            "text" => Some(ColumnType::Text),
            "varchar" | "character varying" => Some(ColumnType::Varchar(None)),
            "char" | "character" => Some(ColumnType::Char(Some(1))),
            "bool" | "boolean" => Some(ColumnType::Bool),
            // SP30: `float` (no precision) is `double precision` in PostgreSQL; the
            // two-word `double precision` is normalized to this single string by the
            // parser before it reaches here. `real`/`float4` is a deferred non-goal.
            "float8" | "float" | "double precision" => Some(ColumnType::Float8),
            // SP32: `numeric`/`decimal` (unconstrained here; typmod added by parser).
            "numeric" | "decimal" => Some(ColumnType::Numeric(None)),
            // SP37: date/time types. `timetz`/`time with time zone` is unsupported (None).
            "date" => Some(ColumnType::Date),
            "time" | "time without time zone" => Some(ColumnType::Time),
            "timestamp" | "timestamp without time zone" => Some(ColumnType::Timestamp),
            "timestamptz" | "timestamp with time zone" => Some(ColumnType::Timestamptz),
            "interval" => Some(ColumnType::Interval),
            // SP40: `bytea` — variable-length binary string.
            "bytea" => Some(ColumnType::Bytea),
            "uuid" => Some(ColumnType::Uuid),
            _ => None,
        }
    }

    pub fn oid(self) -> u32 {
        match self {
            ColumnType::Bool => oids::BOOL,
            ColumnType::Int8 => oids::INT8,
            ColumnType::Int4 => oids::INT4,
            ColumnType::Text => oids::TEXT,
            ColumnType::Varchar(_) => oids::VARCHAR,
            ColumnType::Char(_) => oids::BPCHAR,
            ColumnType::Float8 => oids::FLOAT8,
            ColumnType::Numeric(_) => oids::NUMERIC,
            ColumnType::Date => oids::DATE,
            ColumnType::Time => oids::TIME,
            ColumnType::Timestamp => oids::TIMESTAMP,
            ColumnType::Timestamptz => oids::TIMESTAMPTZ,
            ColumnType::Interval => oids::INTERVAL,
            ColumnType::Bytea => oids::BYTEA,
            ColumnType::Uuid => oids::UUID,
        }
    }

    /// PostgreSQL type name (for error messages and FieldDescription debugging).
    pub fn name(self) -> &'static str {
        match self {
            ColumnType::Bool => "boolean",
            ColumnType::Int8 => "bigint",
            ColumnType::Int4 => "integer",
            ColumnType::Text => "text",
            ColumnType::Varchar(_) => "character varying",
            ColumnType::Char(_) => "character",
            ColumnType::Float8 => "double precision",
            ColumnType::Numeric(_) => "numeric",
            ColumnType::Date => "date",
            ColumnType::Time => "time without time zone",
            ColumnType::Timestamp => "timestamp without time zone",
            ColumnType::Timestamptz => "timestamp with time zone",
            ColumnType::Interval => "interval",
            ColumnType::Bytea => "bytea",
            ColumnType::Uuid => "uuid",
        }
    }

    /// pg_type.typlen: fixed sizes, -1 for variable-length text/numeric.
    pub fn type_size(self) -> i16 {
        match self {
            ColumnType::Bool => 1,
            ColumnType::Int8 => 8,
            ColumnType::Int4 => 4,
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_) => -1,
            ColumnType::Float8 => 8,
            ColumnType::Numeric(_) => -1,
            ColumnType::Date => 4,
            ColumnType::Time => 8,
            ColumnType::Timestamp => 8,
            ColumnType::Timestamptz => 8,
            ColumnType::Interval => 16,
            ColumnType::Bytea => -1,
            ColumnType::Uuid => 16,
        }
    }

    /// True for any `numeric` (ignoring its modifier) — the common "is this the
    /// numeric type?" test used by the promotion/cast logic.
    pub fn is_numeric(self) -> bool {
        matches!(self, ColumnType::Numeric(_))
    }

    pub fn is_string(self) -> bool {
        matches!(
            self,
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_)
        )
    }

    pub fn typmod(self) -> i32 {
        match self {
            ColumnType::Varchar(Some(n)) | ColumnType::Char(Some(n)) => i32::from(n) + 4,
            _ => -1,
        }
    }
}

/// A runtime value.
///
/// `PartialEq`/`Eq`/`Hash` are **hand-written** (SP30), not derived, because of the
/// `Float8` variant: a raw `f64` is not `Eq`/`Hash` (`NaN != NaN`; `-0.0` and `+0.0`
/// have distinct bit patterns yet compare equal). We instead implement PostgreSQL's
/// *grouping* equality (the `float8` btree equality `GROUP BY`/`DISTINCT` use): all
/// `NaN`s are one value, and `-0.0 == +0.0`. The four non-float variants behave exactly
/// as the old derive did. This keys `GROUP BY` group maps and aggregate `DISTINCT` sets.
///
/// SP37 adds five date/time variants using `jiff` types. Their `PartialEq`/`Hash` arms
/// are added in Task 3 (grouping equality); for now they use the `_ => false` catch-all
/// in `PartialEq` and real `Hash` arms (required because `Hash` is exhaustive).
#[derive(Debug, Clone)]
pub enum Datum {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
    /// SP30: PostgreSQL `double precision`.
    Float8(f64),
    /// SP32: PostgreSQL `numeric` — arbitrary-precision exact decimal.
    Numeric(BigDecimal),
    /// SP37: PostgreSQL `date` — a calendar date (no time-of-day, no timezone).
    Date(jiff::civil::Date),
    /// SP37: PostgreSQL `time without time zone` — time-of-day only.
    Time(jiff::civil::Time),
    /// SP37: PostgreSQL `timestamp without time zone` — date + time-of-day, no timezone.
    Timestamp(jiff::civil::DateTime),
    /// SP37: PostgreSQL `timestamp with time zone` — an instant in UTC.
    Timestamptz(jiff::Timestamp),
    /// SP37: PostgreSQL `interval` — months + days + microseconds.
    Interval(crate::datetime::Interval),
    /// SP40: PostgreSQL `bytea` — variable-length binary string (raw bytes).
    Bytea(Vec<u8>),
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Bool(a), Datum::Bool(b)) => a == b,
            (Datum::Int4(a), Datum::Int4(b)) => a == b,
            (Datum::Int8(a), Datum::Int8(b)) => a == b,
            (Datum::Text(a), Datum::Text(b)) => a == b,
            // Grouping equality: `NaN == NaN` (Rust's `==` says false, hence the
            // explicit NaN arm) and `-0.0 == +0.0` (Rust's `==` already says true).
            (Datum::Float8(a), Datum::Float8(b)) => a == b || (a.is_nan() && b.is_nan()),
            // SP32: numeric grouping equality is by VALUE, ignoring scale, so
            // `1.0` and `1.00` group together (`bigdecimal`'s `==` already does this).
            (Datum::Numeric(a), Datum::Numeric(b)) => a == b,
            // SP37: jiff civil types implement PartialEq by calendar/clock value.
            (Datum::Date(a), Datum::Date(b)) => a == b,
            (Datum::Time(a), Datum::Time(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            // timestamptz equality is by absolute instant (jiff Timestamp).
            (Datum::Timestamptz(a), Datum::Timestamptz(b)) => a == b,
            // interval uses its canonical-estimate Eq (Task 2).
            (Datum::Interval(a), Datum::Interval(b)) => a == b,
            // SP40: bytea equality is byte-for-byte (matches PostgreSQL's `byteaeq`).
            (Datum::Bytea(a), Datum::Bytea(b)) => a == b,
            _ => false,
        }
    }
}

// Sound: the relation above is reflexive (NaN now equals itself), symmetric, and
// transitive (every NaN is interchangeable; -0.0/+0.0 are interchangeable).
impl Eq for Datum {}

impl std::hash::Hash for Datum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // A per-variant discriminant keeps distinct variants from colliding cheaply.
        core::mem::discriminant(self).hash(state);
        match self {
            Datum::Null => {}
            Datum::Bool(b) => b.hash(state),
            Datum::Int4(n) => n.hash(state),
            Datum::Int8(n) => n.hash(state),
            Datum::Text(s) => s.hash(state),
            // Canonicalize so equal floats hash equally (the Hash/Eq contract): every
            // NaN → one bit pattern; `-0.0` → `+0.0` (whose bits are all zero).
            Datum::Float8(f) => {
                let bits = if f.is_nan() {
                    0x7ff8_0000_0000_0000u64 // canonical quiet NaN
                } else if *f == 0.0 {
                    0u64 // both -0.0 and +0.0 map here
                } else {
                    f.to_bits()
                };
                bits.hash(state);
            }
            // SP32: hash the scale-normalized form so values that compare equal
            // (`1.0` and `1.00`) hash equally (the Hash/Eq contract).
            Datum::Numeric(d) => d.normalized().to_string().hash(state),
            // SP37: jiff types all implement Hash; Interval derives Hash.
            // (Grouping equality arms come in Task 3; Hash arms are required now
            // because the `impl Hash` is exhaustive — no catch-all.)
            Datum::Date(d) => d.hash(state),
            Datum::Time(t) => t.hash(state),
            Datum::Timestamp(dt) => dt.hash(state),
            Datum::Timestamptz(ts) => ts.hash(state),
            Datum::Interval(i) => i.hash(state),
            // SP40: bytea hashes its bytes.
            Datum::Bytea(b) => b.hash(state),
        }
    }
}

impl Datum {
    /// The non-null column type of this value (None for NULL).
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(ColumnType::Bool),
            Datum::Int4(_) => Some(ColumnType::Int4),
            Datum::Int8(_) => Some(ColumnType::Int8),
            Datum::Text(_) => Some(ColumnType::Text),
            Datum::Float8(_) => Some(ColumnType::Float8),
            // The runtime value carries no typmod — it is unconstrained `numeric`.
            Datum::Numeric(_) => Some(ColumnType::Numeric(None)),
            Datum::Date(_) => Some(ColumnType::Date),
            Datum::Time(_) => Some(ColumnType::Time),
            Datum::Timestamp(_) => Some(ColumnType::Timestamp),
            Datum::Timestamptz(_) => Some(ColumnType::Timestamptz),
            Datum::Interval(_) => Some(ColumnType::Interval),
            Datum::Bytea(_) => Some(ColumnType::Bytea),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_type_from_sql_names_and_aliases() {
        assert_eq!(ColumnType::from_sql_name("int4"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("integer"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("INT"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("int8"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("bigint"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("text"), Some(ColumnType::Text));
        assert_eq!(
            ColumnType::from_sql_name("varchar"),
            Some(ColumnType::Varchar(None))
        );
        assert_eq!(
            ColumnType::from_sql_name("character varying"),
            Some(ColumnType::Varchar(None))
        );
        assert_eq!(
            ColumnType::from_sql_name("char"),
            Some(ColumnType::Char(Some(1)))
        );
        assert_eq!(ColumnType::from_sql_name("bool"), Some(ColumnType::Bool));
        assert_eq!(ColumnType::from_sql_name("boolean"), Some(ColumnType::Bool));
        // SP30: float8 spellings (the two-word `double precision` is assembled by the
        // parser; `from_sql_name` matches the normalized single string and is
        // case-insensitive).
        assert_eq!(
            ColumnType::from_sql_name("float8"),
            Some(ColumnType::Float8)
        );
        assert_eq!(ColumnType::from_sql_name("float"), Some(ColumnType::Float8));
        assert_eq!(
            ColumnType::from_sql_name("double precision"),
            Some(ColumnType::Float8)
        );
        assert_eq!(
            ColumnType::from_sql_name("DOUBLE PRECISION"),
            Some(ColumnType::Float8)
        );
        // `real`/`float4` is a deferred non-goal — unknown for now.
        assert_eq!(ColumnType::from_sql_name("real"), None);
        assert_eq!(ColumnType::from_sql_name("widget"), None);
        assert_eq!(ColumnType::from_sql_name("uuid"), Some(ColumnType::Uuid));
    }

    /// `oid` resolves as a type name (drivers' typeinfo queries cast
    /// `NULL::OID`) and aliases the executor's oid representation, `Int4` —
    /// consistent with the catalog's oid-valued columns (`pg_type.oid`,
    /// `pg_namespace.oid`, `pg_type.typnamespace`, …).
    #[test]
    fn oid_type_name_aliases_int4() {
        use assert2::assert;
        assert!(ColumnType::from_sql_name("oid") == Some(ColumnType::Int4));
        assert!(ColumnType::from_sql_name("OID") == Some(ColumnType::Int4));
        assert!(oids::OID == 26);
    }

    #[test]
    fn float8_oid_name_and_size_match_postgres() {
        assert_eq!(ColumnType::Float8.oid(), 701);
        assert_eq!(ColumnType::Float8.name(), "double precision");
        assert_eq!(ColumnType::Float8.type_size(), 8);
        assert_eq!(Datum::Float8(1.5).column_type(), Some(ColumnType::Float8));
    }

    #[test]
    fn float8_grouping_equality_and_hash_match_postgres() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        fn h(d: &Datum) -> u64 {
            let mut s = DefaultHasher::new();
            d.hash(&mut s);
            s.finish()
        }
        // NaN groups with NaN (unlike raw f64 `==`), and equal values hash equally.
        let nan = Datum::Float8(f64::NAN);
        let nan2 = Datum::Float8(f64::from_bits(0x7ff8_0000_0000_0001)); // a different NaN
        assert_eq!(nan, nan2);
        assert_eq!(h(&nan), h(&nan2));
        // -0.0 and +0.0 group together and hash equally.
        let neg0 = Datum::Float8(-0.0);
        let pos0 = Datum::Float8(0.0);
        assert_eq!(neg0, pos0);
        assert_eq!(h(&neg0), h(&pos0));
        // Distinct finite values are distinct.
        assert_ne!(Datum::Float8(1.5), Datum::Float8(2.5));
        // A NaN is NOT equal to a non-NaN finite value: this pins the `&&` in
        // `a == b || (a.is_nan() && b.is_nan())` — under `&&→||` (a.is_nan() ||
        // b.is_nan()) a NaN would spuriously equal any finite value.
        assert_ne!(Datum::Float8(f64::NAN), Datum::Float8(1.0));
        assert_ne!(Datum::Float8(1.0), Datum::Float8(f64::NAN));
        // Cross-variant never equal (and an int and a float never collide as equal).
        assert_ne!(Datum::Float8(1.0), Datum::Int4(1));
    }

    #[test]
    fn column_type_oids_match_postgres() {
        assert_eq!(ColumnType::Bool.oid(), 16);
        assert_eq!(ColumnType::Int8.oid(), 20);
        assert_eq!(ColumnType::Int4.oid(), 23);
        assert_eq!(ColumnType::Text.oid(), 25);
        assert_eq!(ColumnType::Varchar(Some(12)).oid(), 1043);
        assert_eq!(ColumnType::Char(Some(2)).oid(), 1042);
    }

    #[test]
    fn datum_reports_its_column_type() {
        assert_eq!(Datum::Int4(1).column_type(), Some(ColumnType::Int4));
        assert_eq!(Datum::Null.column_type(), None);
    }

    #[test]
    fn column_type_names_match_postgres() {
        assert_eq!(ColumnType::Bool.name(), "boolean");
        assert_eq!(ColumnType::Int4.name(), "integer");
        assert_eq!(ColumnType::Int8.name(), "bigint");
        assert_eq!(ColumnType::Text.name(), "text");
        assert_eq!(ColumnType::Varchar(Some(12)).name(), "character varying");
        assert_eq!(ColumnType::Char(Some(2)).name(), "character");
    }

    #[test]
    fn column_type_sizes_match_pg_typlen() {
        assert_eq!(ColumnType::Bool.type_size(), 1);
        assert_eq!(ColumnType::Int4.type_size(), 4);
        assert_eq!(ColumnType::Int8.type_size(), 8);
        assert_eq!(ColumnType::Text.type_size(), -1); // variable-length
        assert_eq!(ColumnType::Varchar(Some(12)).type_size(), -1);
        assert_eq!(ColumnType::Char(Some(2)).type_size(), -1);
        assert_eq!(ColumnType::Uuid.type_size(), 16);
        assert_eq!(ColumnType::Varchar(Some(12)).typmod(), 16);
        assert_eq!(ColumnType::Char(Some(2)).typmod(), 6);
    }

    #[test]
    fn uuid_oid_name_and_size_match_postgres() {
        assert_eq!(ColumnType::Uuid.oid(), 2950);
        assert_eq!(ColumnType::Uuid.name(), "uuid");
        assert_eq!(ColumnType::Uuid.type_size(), 16);
    }

    #[test]
    fn datetime_oids_names_sizes_match_postgres() {
        assert_eq!(ColumnType::Date.oid(), 1082);
        assert_eq!(ColumnType::Time.oid(), 1083);
        assert_eq!(ColumnType::Timestamp.oid(), 1114);
        assert_eq!(ColumnType::Timestamptz.oid(), 1184);
        assert_eq!(ColumnType::Interval.oid(), 1186);
        assert_eq!(ColumnType::Date.name(), "date");
        assert_eq!(ColumnType::Time.name(), "time without time zone");
        assert_eq!(ColumnType::Timestamp.name(), "timestamp without time zone");
        assert_eq!(ColumnType::Timestamptz.name(), "timestamp with time zone");
        assert_eq!(ColumnType::Interval.name(), "interval");
        assert_eq!(ColumnType::Date.type_size(), 4);
        assert_eq!(ColumnType::Time.type_size(), 8);
        assert_eq!(ColumnType::Timestamp.type_size(), 8);
        assert_eq!(ColumnType::Timestamptz.type_size(), 8);
        assert_eq!(ColumnType::Interval.type_size(), 16);
    }

    #[test]
    fn datetime_datum_grouping_equality_and_hash() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };

        use crate::datetime::Interval;
        fn h(d: &Datum) -> u64 {
            let mut s = DefaultHasher::new();
            d.hash(&mut s);
            s.finish()
        }
        let d1 = Datum::Date(
            "2024-01-15"
                .parse::<jiff::civil::Date>()
                .expect("valid date literal"),
        );
        let d2 = Datum::Date(
            "2024-01-15"
                .parse::<jiff::civil::Date>()
                .expect("valid date literal"),
        );
        assert_eq!(d1, d2);
        assert_eq!(h(&d1), h(&d2));
        let m = Datum::Interval(Interval {
            months: 1,
            days: 0,
            micros: 0,
        });
        let dd = Datum::Interval(Interval {
            months: 0,
            days: 30,
            micros: 0,
        });
        assert_eq!(m, dd);
        assert_eq!(h(&m), h(&dd));
        assert_ne!(
            d1,
            Datum::Timestamp(
                "2024-01-15T00:00:00"
                    .parse::<jiff::civil::DateTime>()
                    .expect("valid datetime literal"),
            )
        );
    }

    #[test]
    fn datetime_type_names_resolve_and_timetz_is_unsupported() {
        assert_eq!(ColumnType::from_sql_name("date"), Some(ColumnType::Date));
        assert_eq!(ColumnType::from_sql_name("time"), Some(ColumnType::Time));
        assert_eq!(
            ColumnType::from_sql_name("time without time zone"),
            Some(ColumnType::Time)
        );
        assert_eq!(
            ColumnType::from_sql_name("timestamp"),
            Some(ColumnType::Timestamp)
        );
        assert_eq!(
            ColumnType::from_sql_name("timestamp without time zone"),
            Some(ColumnType::Timestamp)
        );
        assert_eq!(
            ColumnType::from_sql_name("timestamptz"),
            Some(ColumnType::Timestamptz)
        );
        assert_eq!(
            ColumnType::from_sql_name("timestamp with time zone"),
            Some(ColumnType::Timestamptz)
        );
        assert_eq!(
            ColumnType::from_sql_name("interval"),
            Some(ColumnType::Interval)
        );
        assert_eq!(ColumnType::from_sql_name("timetz"), None);
        assert_eq!(ColumnType::from_sql_name("time with time zone"), None);
    }

    /// SP37 mutation-killer: the `(Timestamptz, Timestamptz)` arm of `Datum`'s
    /// `PartialEq` + `Hash` — two timestamptz Datums at the SAME absolute instant
    /// (parsed from different wall-clock/offset spellings) are EQUAL and hash-equal,
    /// and two at DIFFERENT instants are unequal. Pins the deleted-arm (#147),
    /// `== → !=` (#148), and `hash with ()` (#149) mutants. The existing
    /// `datetime_datum_grouping_equality_and_hash` covers Date/Interval but not
    /// Timestamptz distinctly.
    #[test]
    fn timestamptz_datum_equality_and_hash_by_instant() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        fn h(d: &Datum) -> u64 {
            let mut s = DefaultHasher::new();
            d.hash(&mut s);
            s.finish()
        }
        let tz = jiff::tz::TimeZone::UTC;
        // 12:00+00 and 14:00+02 denote the SAME instant (both 12:00 UTC).
        let a = Datum::Timestamptz(
            crate::datetime::parse_timestamptz("2024-01-15 12:00:00+00", &tz).expect("a"),
        );
        let b = Datum::Timestamptz(
            crate::datetime::parse_timestamptz("2024-01-15 14:00:00+02", &tz).expect("b"),
        );
        assert_eq!(a, b, "same absolute instant compares equal");
        assert_eq!(h(&a), h(&b), "equal instants hash equally");
        // A different instant is unequal (kills `== → !=`, which would make these
        // two — same instant — UNequal and make a different instant EQUAL).
        let c = Datum::Timestamptz(
            crate::datetime::parse_timestamptz("2024-01-15 13:00:00+00", &tz).expect("c"),
        );
        assert_ne!(a, c, "a one-hour-later instant is not equal");
        // Distinct-instant hashes differ (kills `hash with ()`, which collapses all
        // Timestamptz to one hash). Two distinct instants must hash differently.
        assert_ne!(h(&a), h(&c), "distinct instants hash differently");
    }

    /// Pins three SP32 `numeric` lines that a full-file mutation sweep flagged as
    /// uncovered: the `numeric`/`decimal` name arm (from_sql_name), the `-1`
    /// variable typlen for `numeric`, and the `(Numeric, Numeric)` equality arm.
    #[test]
    fn bytea_text_is_hex_format() {
        let d = Datum::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(d.column_type(), Some(ColumnType::Bytea));
        assert_eq!(
            crate::encoding::encode_text(&d, &jiff::tz::TimeZone::UTC),
            b"\\xdeadbeef"
        );
        assert_eq!(
            crate::encoding::encode_binary(&d),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(ColumnType::from_sql_name("bytea"), Some(ColumnType::Bytea));
        // type_size is -1 (variable-length), NOT a positive size; kills `delete -`.
        assert_eq!(ColumnType::Bytea.type_size(), -1i16);
        // Bytea equality is byte-for-byte; kills `delete arm` and `== → !=`.
        let same = Datum::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let diff = Datum::Bytea(vec![0x00]);
        assert_eq!(d, same, "identical byte sequences are equal");
        assert_ne!(d, diff, "different byte sequences are not equal");
    }

    #[test]
    fn numeric_column_type_name_size_and_equality() {
        use std::str::FromStr;

        use bigdecimal::BigDecimal;
        // from_sql_name: both `numeric` and `decimal` resolve (kills the deleted arm).
        assert_eq!(
            ColumnType::from_sql_name("numeric"),
            Some(ColumnType::Numeric(None))
        );
        assert_eq!(
            ColumnType::from_sql_name("decimal"),
            Some(ColumnType::Numeric(None))
        );
        // type_size: numeric is variable-length (-1), NOT a fixed positive size
        // (kills `delete -` which would make it +1).
        assert_eq!(ColumnType::Numeric(None).type_size(), -1);
        // (Numeric, Numeric) equality compares by value, ignoring scale (kills the
        // deleted arm, which would fall to `_ => false` and make equal values
        // UNequal).
        let a = Datum::Numeric(BigDecimal::from_str("1.0").expect("1.0"));
        let b = Datum::Numeric(BigDecimal::from_str("1.00").expect("1.00"));
        assert_eq!(a, b, "numeric equality is by value, ignoring scale");
        let c = Datum::Numeric(BigDecimal::from_str("2.0").expect("2.0"));
        assert_ne!(a, c);
    }
}
