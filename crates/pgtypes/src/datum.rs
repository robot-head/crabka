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
    /// PostgreSQL `regclass` — a relation's `pg_class` oid with name-based
    /// text input; values live in the `Int4` datum like `oid`.
    pub const REGCLASS: u32 = 2205;
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
    /// PostgreSQL `json` — accepted on input (parameters, casts) as an alias for
    /// `jsonb`; crabka never *reports* this OID.
    pub const JSON: u32 = 114;
    /// PostgreSQL `jsonb` — the decomposed binary JSON type.
    pub const JSONB: u32 = 3802;
    /// `json[]`.
    pub const JSONARRAY: u32 = 199;
    /// `jsonb[]`.
    pub const JSONBARRAY: u32 = 3807;
    /// `boolean[]`.
    pub const BOOLARRAY: u32 = 1000;
    /// `bytea[]`.
    pub const BYTEAARRAY: u32 = 1001;
    /// `bigint[]`.
    pub const INT8ARRAY: u32 = 1016;
    /// `integer[]`.
    pub const INT4ARRAY: u32 = 1007;
    /// `text[]`.
    pub const TEXTARRAY: u32 = 1009;
    /// `double precision[]`.
    pub const FLOAT8ARRAY: u32 = 1022;
    /// `numeric[]`.
    pub const NUMERICARRAY: u32 = 1231;
    /// `date[]`.
    pub const DATEARRAY: u32 = 1182;
    /// `time without time zone[]`.
    pub const TIMEARRAY: u32 = 1183;
    /// `timestamp without time zone[]`.
    pub const TIMESTAMPARRAY: u32 = 1115;
    /// `timestamp with time zone[]`.
    pub const TIMESTAMPTZARRAY: u32 = 1185;
    /// `interval[]`.
    pub const INTERVALARRAY: u32 = 1187;
    /// `uuid[]`.
    pub const UUIDARRAY: u32 = 2951;
}

/// The element type of a one-dimensional SQL array.
///
/// A separate `Copy` enum rather than a boxed [`ColumnType`] because
/// `ColumnType` is passed by value throughout the executor; it is deliberately
/// smaller than `ColumnType` — the types with a modifier (`varchar(n)`,
/// `char(n)`), `regclass`, and arrays themselves have no array form here and are
/// refused with 0A000 by [`ElemType::from_column_type`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElemType {
    Bool,
    Int4,
    Int8,
    Text,
    Float8,
    Numeric,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Interval,
    Bytea,
    Uuid,
    Jsonb,
}

impl ElemType {
    /// Every supported array element type, in `code()` order.
    pub const ALL: [ElemType; 14] = [
        ElemType::Bool,
        ElemType::Int4,
        ElemType::Int8,
        ElemType::Text,
        ElemType::Float8,
        ElemType::Numeric,
        ElemType::Date,
        ElemType::Time,
        ElemType::Timestamp,
        ElemType::Timestamptz,
        ElemType::Interval,
        ElemType::Bytea,
        ElemType::Uuid,
        ElemType::Jsonb,
    ];

    /// The element type as a column type (`numeric` is unconstrained — an array
    /// carries no element typmod).
    pub fn column_type(self) -> ColumnType {
        match self {
            ElemType::Bool => ColumnType::Bool,
            ElemType::Int4 => ColumnType::Int4,
            ElemType::Int8 => ColumnType::Int8,
            ElemType::Text => ColumnType::Text,
            ElemType::Float8 => ColumnType::Float8,
            ElemType::Numeric => ColumnType::Numeric(None),
            ElemType::Date => ColumnType::Date,
            ElemType::Time => ColumnType::Time,
            ElemType::Timestamp => ColumnType::Timestamp,
            ElemType::Timestamptz => ColumnType::Timestamptz,
            ElemType::Interval => ColumnType::Interval,
            ElemType::Bytea => ColumnType::Bytea,
            ElemType::Uuid => ColumnType::Uuid,
            ElemType::Jsonb => ColumnType::Jsonb,
        }
    }

    /// The element type for `elem`, or `None` when crabka has no array type for
    /// it (`varchar(n)`, `char(n)`, `regclass`, and nested arrays).
    pub fn from_column_type(elem: ColumnType) -> Option<Self> {
        Some(match elem {
            ColumnType::Bool => ElemType::Bool,
            ColumnType::Int4 => ElemType::Int4,
            ColumnType::Int8 => ElemType::Int8,
            ColumnType::Text => ElemType::Text,
            ColumnType::Float8 => ElemType::Float8,
            ColumnType::Numeric(_) => ElemType::Numeric,
            ColumnType::Date => ElemType::Date,
            ColumnType::Time => ElemType::Time,
            ColumnType::Timestamp => ElemType::Timestamp,
            ColumnType::Timestamptz => ElemType::Timestamptz,
            ColumnType::Interval => ElemType::Interval,
            ColumnType::Bytea => ElemType::Bytea,
            ColumnType::Uuid => ElemType::Uuid,
            ColumnType::Jsonb => ElemType::Jsonb,
            ColumnType::Varchar(_)
            | ColumnType::Char(_)
            | ColumnType::Regclass
            | ColumnType::Array(_) => return None,
        })
    }

    /// The element type's own OID (`pg_type.typelem` of the array type).
    pub fn oid(self) -> u32 {
        self.column_type().oid()
    }

    /// The OID of the array type over this element type (`pg_type.typarray`).
    pub fn array_oid(self) -> u32 {
        match self {
            ElemType::Bool => oids::BOOLARRAY,
            ElemType::Int4 => oids::INT4ARRAY,
            ElemType::Int8 => oids::INT8ARRAY,
            ElemType::Text => oids::TEXTARRAY,
            ElemType::Float8 => oids::FLOAT8ARRAY,
            ElemType::Numeric => oids::NUMERICARRAY,
            ElemType::Date => oids::DATEARRAY,
            ElemType::Time => oids::TIMEARRAY,
            ElemType::Timestamp => oids::TIMESTAMPARRAY,
            ElemType::Timestamptz => oids::TIMESTAMPTZARRAY,
            ElemType::Interval => oids::INTERVALARRAY,
            ElemType::Bytea => oids::BYTEAARRAY,
            ElemType::Uuid => oids::UUIDARRAY,
            ElemType::Jsonb => oids::JSONBARRAY,
        }
    }

    /// The element type's PostgreSQL name (`integer`, `text`, …).
    pub fn name(self) -> &'static str {
        self.column_type().name()
    }

    /// The array type's PostgreSQL name (`integer[]`, `text[]`, …).
    pub fn array_name(self) -> &'static str {
        match self {
            ElemType::Bool => "boolean[]",
            ElemType::Int4 => "integer[]",
            ElemType::Int8 => "bigint[]",
            ElemType::Text => "text[]",
            ElemType::Float8 => "double precision[]",
            ElemType::Numeric => "numeric[]",
            ElemType::Date => "date[]",
            ElemType::Time => "time without time zone[]",
            ElemType::Timestamp => "timestamp without time zone[]",
            ElemType::Timestamptz => "timestamp with time zone[]",
            ElemType::Interval => "interval[]",
            ElemType::Bytea => "bytea[]",
            ElemType::Uuid => "uuid[]",
            ElemType::Jsonb => "jsonb[]",
        }
    }

    /// A stable, **append-only** wire/storage code. Persisted by the row encoder
    /// and the catalog's schema serializer, so existing values must never change.
    pub fn code(self) -> u8 {
        match self {
            ElemType::Bool => 0,
            ElemType::Int4 => 1,
            ElemType::Int8 => 2,
            ElemType::Text => 3,
            ElemType::Float8 => 4,
            ElemType::Numeric => 5,
            ElemType::Date => 6,
            ElemType::Time => 7,
            ElemType::Timestamp => 8,
            ElemType::Timestamptz => 9,
            ElemType::Interval => 10,
            ElemType::Bytea => 11,
            ElemType::Uuid => 12,
            ElemType::Jsonb => 13,
        }
    }

    /// The inverse of [`ElemType::code`] (`None` for an unknown code).
    pub fn from_code(code: u8) -> Option<Self> {
        ElemType::ALL.into_iter().find(|e| e.code() == code)
    }

    /// The element type of an array OID (`pg_type.typelem`), for parameter
    /// binding. `json[]` maps onto `jsonb[]` like `json` maps onto `jsonb`.
    pub fn from_array_oid(oid: u32) -> Option<Self> {
        if oid == oids::JSONARRAY {
            return Some(ElemType::Jsonb);
        }
        ElemType::ALL.into_iter().find(|e| e.array_oid() == oid)
    }
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
    /// PostgreSQL `regclass` (OID 2205) — a relation's `pg_class` oid. Values
    /// are `Datum::Int4` like `oid`; what distinguishes the type is input
    /// conversion (a non-numeric string is a relation name needing catalog
    /// resolution, which the session/executor layers perform — the pure
    /// datum-parse path only accepts numeric strings).
    Regclass,
    /// PostgreSQL `jsonb` (OID 3802) — decomposed JSON. `json` (114) is accepted
    /// on input as an alias but never reported.
    Jsonb,
    /// A one-dimensional PostgreSQL array (OID = the element type's `typarray`).
    Array(ElemType),
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
            "regclass" => Some(ColumnType::Regclass),
            // `json` is an input alias for `jsonb`: values are stored decomposed
            // and always report OID 3802 (a documented divergence).
            "jsonb" | "json" => Some(ColumnType::Jsonb),
            _ => None,
        }
    }

    /// The one-dimensional array type over `elem`, or `None` when crabka has no
    /// array type for it (`varchar(n)`, `char(n)`, `regclass`, nested arrays) —
    /// callers report that as 0A000.
    pub fn array_of(elem: ColumnType) -> Option<Self> {
        ElemType::from_column_type(elem).map(ColumnType::Array)
    }

    /// The element type when this is an array type.
    pub fn array_element(self) -> Option<ElemType> {
        match self {
            ColumnType::Array(elem) => Some(elem),
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
            ColumnType::Regclass => oids::REGCLASS,
            ColumnType::Jsonb => oids::JSONB,
            ColumnType::Array(elem) => elem.array_oid(),
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
            ColumnType::Regclass => "regclass",
            ColumnType::Jsonb => "jsonb",
            ColumnType::Array(elem) => elem.array_name(),
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
            ColumnType::Regclass => 4,
            // jsonb and arrays are variable-length.
            ColumnType::Jsonb | ColumnType::Array(_) => -1,
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
    /// PostgreSQL `jsonb` — a decomposed JSON value in canonical form.
    Jsonb(crate::jsonb::JsonbValue),
    /// A one-dimensional PostgreSQL array.
    Array(ArrayValue),
}

/// A one-dimensional array value.
///
/// The element type is carried alongside the elements so an empty array is still
/// typed (`'{}'::int[]` knows it is `integer[]`) and so the binary wire encoding,
/// which must emit the element OID, is context-free.
#[derive(Debug, Clone)]
pub struct ArrayValue {
    /// The array's element type.
    pub elem: ElemType,
    /// The elements, in order; `Datum::Null` is a NULL element.
    pub elems: Vec<Datum>,
}

impl ArrayValue {
    /// An array of `elems` with element type `elem`.
    pub fn new(elem: ElemType, elems: Vec<Datum>) -> Self {
        ArrayValue { elem, elems }
    }

    /// The array's column type.
    pub fn column_type(&self) -> ColumnType {
        ColumnType::Array(self.elem)
    }
}

impl PartialEq for ArrayValue {
    fn eq(&self, other: &Self) -> bool {
        self.elem == other.elem && self.elems == other.elems
    }
}

impl Eq for ArrayValue {}

impl std::hash::Hash for ArrayValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.elem.hash(state);
        self.elems.hash(state);
    }
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
            // jsonb equality is structural over the canonical form (key order is
            // already normalized; number scale is ignored, as in `numeric`).
            (Datum::Jsonb(a), Datum::Jsonb(b)) => a == b,
            // Arrays are equal when their element type and every element are.
            (Datum::Array(a), Datum::Array(b)) => a == b,
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
            // SP37: jiff types all implement Hash by value. `Interval` hashes its
            // `canonical_micros` — the same quantity its `PartialEq` compares — so
            // `1 mon` and `30 days` hash alike (the Hash/Eq contract).
            Datum::Date(d) => d.hash(state),
            Datum::Time(t) => t.hash(state),
            Datum::Timestamp(dt) => dt.hash(state),
            Datum::Timestamptz(ts) => ts.hash(state),
            Datum::Interval(i) => i.hash(state),
            // SP40: bytea hashes its bytes.
            Datum::Bytea(b) => b.hash(state),
            // Both hash scale-normalized numbers internally, matching `Eq`.
            Datum::Jsonb(j) => j.hash(state),
            Datum::Array(a) => a.hash(state),
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
            Datum::Jsonb(_) => Some(ColumnType::Jsonb),
            Datum::Array(a) => Some(a.column_type()),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }
}

/// Normalize a value into the form used for **index key bytes**.
///
/// Index lookups are equality-by-bytes over `encode_row`, so two values that
/// compare equal must encode identically. Four representations break that on
/// their own: `numeric` scale (`1.0` vs `1.00`), float negative zero, `interval`
/// field spelling (`1 mon` vs `30 days`), and the same inside `jsonb` and array
/// elements. (Object key order is not a concern — `jsonb` is stored canonically
/// ordered.)
///
/// This is the **key** form only. Row storage keeps every value exactly as
/// given, so a stored `interval '1 mon'` still renders `1 mon`, not `30 days`.
///
/// Returns `Cow::Borrowed` when the value is already canonical, which is the
/// common case.
#[must_use]
pub fn canonicalize_for_key(value: &Datum) -> std::borrow::Cow<'_, Datum> {
    use std::borrow::Cow;
    match value {
        Datum::Numeric(d) => {
            let normalized = crate::numeric::canonical(d.normalized());
            if normalized.fractional_digit_count() == d.fractional_digit_count() {
                Cow::Borrowed(value)
            } else {
                Cow::Owned(Datum::Numeric(normalized))
            }
        }
        // `-0.0 == 0.0` and every NaN is one value under `Datum`'s grouping
        // equality, but their bit patterns differ.
        Datum::Float8(f) if f.is_nan() => Cow::Owned(Datum::Float8(f64::NAN)),
        Datum::Float8(f) if *f == 0.0 && f.is_sign_negative() => Cow::Owned(Datum::Float8(0.0)),
        // `interval` equality (and `Hash`, and `Ord`) is PostgreSQL's canonical
        // estimate — a 30-day month and a 24-hour day — so `1 mon`, `30 days`
        // and `720 hours` are ONE value with three field spellings, while the
        // encoding stores months/days/micros separately. `justify_interval` is
        // PostgreSQL's canonical spelling of that estimate: it preserves
        // `Interval::canonical_micros` and depends on nothing else, so equal
        // intervals justify to identical fields.
        Datum::Interval(iv) => match crate::datetime::justify_interval(*iv) {
            // An interval whose justified form overflows `i32` months has no
            // in-range canonical spelling (only reachable from raw binary
            // input, since every parse path range-checks); leave it as-is
            // rather than picking a lossy stand-in.
            Err(_) => Cow::Borrowed(value),
            Ok(justified) => {
                if (justified.months, justified.days, justified.micros)
                    == (iv.months, iv.days, iv.micros)
                {
                    Cow::Borrowed(value)
                } else {
                    Cow::Owned(Datum::Interval(justified))
                }
            }
        },
        Datum::Jsonb(j) => match j.normalized_numbers() {
            Some(normalized) => Cow::Owned(Datum::Jsonb(normalized)),
            None => Cow::Borrowed(value),
        },
        Datum::Array(a) => {
            let mut changed = false;
            let elems = a
                .elems
                .iter()
                .map(|e| match canonicalize_for_key(e) {
                    Cow::Owned(v) => {
                        changed = true;
                        v
                    }
                    Cow::Borrowed(v) => v.clone(),
                })
                .collect();
            if changed {
                Cow::Owned(Datum::Array(ArrayValue::new(a.elem, elems)))
            } else {
                Cow::Borrowed(value)
            }
        }
        _ => Cow::Borrowed(value),
    }
}

/// [`canonicalize_for_key`] over a whole index key tuple, borrowing when every
/// value is already canonical.
#[must_use]
pub fn canonicalize_row_for_key(values: &[Datum]) -> std::borrow::Cow<'_, [Datum]> {
    use std::borrow::Cow;
    let canonical: Vec<Cow<'_, Datum>> = values.iter().map(canonicalize_for_key).collect();
    if canonical.iter().all(|v| matches!(v, Cow::Borrowed(_))) {
        return Cow::Borrowed(values);
    }
    Cow::Owned(canonical.into_iter().map(Cow::into_owned).collect())
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

    // ---- jsonb + arrays ----

    fn jsonb(text: &str) -> Datum {
        Datum::Jsonb(crate::jsonb::parse(text).expect("valid jsonb"))
    }

    fn array(elem: ElemType, elems: Vec<Datum>) -> Datum {
        Datum::Array(ArrayValue::new(elem, elems))
    }

    fn hash_of(d: &Datum) -> u64 {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        let mut hasher = DefaultHasher::new();
        d.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn jsonb_column_type_reports_postgres_oid_name_and_size() {
        use assert2::assert;
        assert!(ColumnType::Jsonb.oid() == 3802);
        assert!(ColumnType::Jsonb.name() == "jsonb");
        assert!(ColumnType::Jsonb.type_size() == -1);
        assert!(ColumnType::Jsonb.typmod() == -1);
        assert!(ColumnType::from_sql_name("jsonb") == Some(ColumnType::Jsonb));
        // `json` is an input alias that reports as jsonb.
        assert!(ColumnType::from_sql_name("JSON") == Some(ColumnType::Jsonb));
        assert!(jsonb("1").column_type() == Some(ColumnType::Jsonb));
    }

    #[test]
    fn array_types_report_the_postgres_typarray_oids() {
        use assert2::assert;
        let expected: &[(ElemType, u32, u32, &str)] = &[
            (ElemType::Bool, 16, 1000, "boolean[]"),
            (ElemType::Bytea, 17, 1001, "bytea[]"),
            (ElemType::Int8, 20, 1016, "bigint[]"),
            (ElemType::Int4, 23, 1007, "integer[]"),
            (ElemType::Text, 25, 1009, "text[]"),
            (ElemType::Float8, 701, 1022, "double precision[]"),
            (ElemType::Numeric, 1700, 1231, "numeric[]"),
            (ElemType::Date, 1082, 1182, "date[]"),
            (ElemType::Time, 1083, 1183, "time without time zone[]"),
            (
                ElemType::Timestamp,
                1114,
                1115,
                "timestamp without time zone[]",
            ),
            (
                ElemType::Timestamptz,
                1184,
                1185,
                "timestamp with time zone[]",
            ),
            (ElemType::Interval, 1186, 1187, "interval[]"),
            (ElemType::Uuid, 2950, 2951, "uuid[]"),
            (ElemType::Jsonb, 3802, 3807, "jsonb[]"),
        ];
        assert!(expected.len() == ElemType::ALL.len());
        for (elem, elem_oid, array_oid, name) in expected {
            assert!(elem.oid() == *elem_oid, "{elem:?} element oid");
            assert!(elem.array_oid() == *array_oid, "{elem:?} array oid");
            assert!(elem.array_name() == *name, "{elem:?} array name");
            let ty = ColumnType::Array(*elem);
            assert!(ty.oid() == *array_oid);
            assert!(ty.name() == *name);
            assert!(ty.type_size() == -1);
            assert!(ty.array_element() == Some(*elem));
            assert!(ElemType::from_array_oid(*array_oid) == Some(*elem));
        }
        // `json[]` binds onto `jsonb[]`, like `json` onto `jsonb`.
        assert!(ElemType::from_array_oid(oids::JSONARRAY) == Some(ElemType::Jsonb));
        assert!(ElemType::from_array_oid(9999) == None);
    }

    /// The element codes are persisted (row encoding, catalog schema), so they
    /// are append-only: this pins every existing value.
    #[test]
    fn element_type_codes_are_stable_and_round_trip() {
        use assert2::assert;
        for (code, elem) in ElemType::ALL.iter().enumerate() {
            let code = u8::try_from(code).expect("small");
            assert!(elem.code() == code, "{elem:?}");
            assert!(ElemType::from_code(code) == Some(*elem));
        }
        assert!(ElemType::from_code(200) == None);
    }

    #[test]
    fn array_of_refuses_element_types_without_an_array_type() {
        use assert2::assert;
        assert!(ColumnType::array_of(ColumnType::Int4) == Some(ColumnType::Array(ElemType::Int4)));
        assert!(
            ColumnType::array_of(ColumnType::Numeric(Some(Typmod {
                precision: 5,
                scale: 2,
            }))) == Some(ColumnType::Array(ElemType::Numeric)),
            "an array element carries no typmod"
        );
        for unsupported in [
            ColumnType::Varchar(Some(8)),
            ColumnType::Varchar(None),
            ColumnType::Char(Some(2)),
            ColumnType::Regclass,
            ColumnType::Array(ElemType::Int4),
        ] {
            assert!(
                ColumnType::array_of(unsupported) == None,
                "{unsupported:?} has no array type"
            );
        }
        assert!(ColumnType::Int4.array_element() == None);
    }

    #[test]
    fn jsonb_and_array_datums_use_structural_equality() {
        use assert2::assert;
        // jsonb: key order and number scale do not distinguish values.
        assert!(jsonb(r#"{"b":2,"a":1}"#) == jsonb(r#"{"a":1,"b":2}"#));
        assert!(jsonb(r#"{"a":1.0}"#) == jsonb(r#"{"a":1.00}"#));
        assert!(jsonb("1") != jsonb("2"));
        // A jsonb null is not a SQL NULL, and never equals another type.
        assert!(jsonb("null") != Datum::Null);
        assert!(jsonb("1") != Datum::Int4(1));
        // Arrays: element type and elements both matter.
        let ints = array(ElemType::Int4, vec![Datum::Int4(1)]);
        assert!(ints == array(ElemType::Int4, vec![Datum::Int4(1)]));
        assert!(ints != array(ElemType::Int4, vec![Datum::Int4(2)]));
        assert!(ints != array(ElemType::Int4, vec![Datum::Int4(1), Datum::Null]));
        assert!(
            array(ElemType::Int4, vec![]) != array(ElemType::Text, vec![]),
            "an empty array is still typed"
        );
        assert!(ints.column_type() == Some(ColumnType::Array(ElemType::Int4)));
    }

    /// The index-key contract: equal Datums must hash equally AND canonicalize
    /// to identical `encode_row` input, because index lookups are equality by
    /// raw key bytes.
    #[test]
    fn equal_datums_hash_equally_and_canonicalize_identically() {
        use assert2::assert;
        let num = |s: &str| Datum::Numeric(crate::numeric::parse(s).expect("numeric"));
        let iv = |s: &str| Datum::Interval(crate::datetime::parse_interval(s).expect("interval"));
        let pairs: &[(Datum, Datum)] = &[
            // Plain numeric scale — the latent case this fixes.
            (num("1.0"), num("1.00")),
            (num("100"), num("1e2")),
            (num("-0.0"), num("0")),
            // Float negative zero and NaN bit patterns.
            (Datum::Float8(-0.0), Datum::Float8(0.0)),
            (
                Datum::Float8(f64::NAN),
                Datum::Float8(f64::from_bits(0x7ff8_0000_0000_0001)),
            ),
            // interval: equality is the 30-day-month / 24-hour-day estimate, so
            // the same value has many field spellings.
            (iv("1 mon"), iv("30 days")),
            (iv("1 day"), iv("24 hours")),
            (iv("1 year"), iv("12 mons")),
            (iv("1 mon"), iv("720 hours")),
            (iv("1 mon -1 hour"), iv("29 days 23:00:00")),
            (iv("-1 mon"), iv("-30 days")),
            (iv("-1 day 1 hour"), iv("-23 hours")),
            (iv("0 days"), iv("00:00:00")),
            // jsonb: key order (storage invariant) and number scale.
            (jsonb(r#"{"b":2,"a":1}"#), jsonb(r#"{"a":1,"b":2}"#)),
            (jsonb(r#"{"a":1.0}"#), jsonb(r#"{"a":1.00}"#)),
            (jsonb(r#"[1.0, {"k": 2.00}]"#), jsonb(r#"[1, {"k": 2}]"#)),
            // Array elements canonicalize recursively.
            (
                array(ElemType::Numeric, vec![num("1.0"), Datum::Null]),
                array(ElemType::Numeric, vec![num("1.000"), Datum::Null]),
            ),
            (
                array(ElemType::Jsonb, vec![jsonb(r#"{"b":1,"a":2}"#)]),
                array(ElemType::Jsonb, vec![jsonb(r#"{"a":2,"b":1}"#)]),
            ),
            (
                array(ElemType::Interval, vec![iv("1 mon"), Datum::Null]),
                array(ElemType::Interval, vec![iv("30 days"), Datum::Null]),
            ),
            // Already-canonical values are unchanged (and still agree).
            (Datum::Text("x".into()), Datum::Text("x".into())),
            (array(ElemType::Int4, vec![]), array(ElemType::Int4, vec![])),
        ];
        for (left, right) in pairs {
            assert!(left == right, "{left:?} == {right:?}");
            assert!(hash_of(left) == hash_of(right), "hash of {left:?}");
            let (a, b) = (canonicalize_for_key(left), canonicalize_for_key(right));
            assert!(*a == *b, "canonical form of {left:?}");
            assert!(
                crate::encoding::encode_text(&a, &jiff::tz::TimeZone::UTC)
                    == crate::encoding::encode_text(&b, &jiff::tz::TimeZone::UTC),
                "canonical text of {left:?}"
            );
        }
    }

    /// The converse of the key contract for `interval`: values that are NOT
    /// equal keep distinct canonical spellings, and a value with no in-range
    /// canonical spelling is left alone rather than overflowing.
    #[test]
    fn interval_canonicalization_keeps_unequal_values_apart() {
        use std::borrow::Cow;

        use assert2::assert;
        let iv = |s: &str| Datum::Interval(crate::datetime::parse_interval(s).expect("interval"));
        let canonical_text = |d: &Datum| {
            crate::encoding::encode_text(&canonicalize_for_key(d), &jiff::tz::TimeZone::UTC)
        };
        for (left, right) in [
            (iv("1 mon"), iv("31 days")),
            (iv("1 day"), iv("25 hours")),
            (iv("1 mon"), iv("-1 mon")),
        ] {
            assert!(left != right, "{left:?} != {right:?}");
            assert!(
                canonical_text(&left) != canonical_text(&right),
                "canonical form of {left:?} vs {right:?}"
            );
        }
        // No in-range justified spelling exists for this one (its canonical
        // month count overflows i32), so it is left exactly as given.
        let unjustifiable = Datum::Interval(crate::datetime::Interval {
            months: i32::MAX,
            days: i32::MAX,
            micros: i64::MAX,
        });
        assert!(matches!(
            canonicalize_for_key(&unjustifiable),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn canonicalize_borrows_canonical_values_and_owns_normalized_ones() {
        use std::borrow::Cow;

        use assert2::assert;
        for already_canonical in [
            Datum::Null,
            Datum::Int4(1),
            Datum::Text("x".into()),
            Datum::Float8(1.5),
            Datum::Numeric(crate::numeric::parse("1.5").expect("n")),
            Datum::Interval(crate::datetime::parse_interval("1 mon").expect("interval")),
            jsonb(r#"{"a": 1}"#),
            array(ElemType::Int4, vec![Datum::Int4(1), Datum::Null]),
        ] {
            assert!(
                matches!(canonicalize_for_key(&already_canonical), Cow::Borrowed(_)),
                "{already_canonical:?} is already canonical"
            );
        }
        let row = [Datum::Int4(1), Datum::Text("x".into())];
        assert!(matches!(canonicalize_row_for_key(&row), Cow::Borrowed(_)));
        let scaled = [Datum::Numeric(crate::numeric::parse("1.50").expect("n"))];
        assert!(matches!(canonicalize_row_for_key(&scaled), Cow::Owned(_)));
        assert!(
            canonicalize_row_for_key(&scaled)[0]
                == Datum::Numeric(crate::numeric::parse("1.5").expect("n"))
        );
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
