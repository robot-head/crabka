//! Versioned (de)serialization of a table schema — the value stored under
//! `crabka_pgkv::key::catalog_key(name)`. Format: version byte, `table_id`
//! (u32 BE), column count (u32 BE), then per column: u32 name length, name bytes,
//! type tag; table option flags (u8); followed by a `foreign` flag byte: `0` =
//! ordinary table (no further payload), `1` = foreign table (server name len u32,
//! server name bytes, option count u32, then per option: key len u32, key bytes,
//! value len u32, value bytes).
//!
//! Foreign-data-wrapper, foreign-server, and user-mapping objects use their own
//! simple binary format (not the schema format).

use crabka_pgkv::KvError;
use crabka_pgtypes::{
    ColumnType, Datum,
    numeric::Typmod,
    usertype::{CompositeField, DomainBody, DomainCheck, RangeBody, UserType, UserTypeBody},
};

use crate::{
    CheckConstraint, Column, ColumnDefault, ExclusionOperator, ForeignDataWrapper, ForeignKey,
    ForeignServer, ForeignTableMeta, HashSharding, IdentityKind, Index, IndexConstraint, IndexMethod,
    IndexPlacement, MatchType, ReferentialAction, Sequence, ShardingStrategy, TableOptions,
    UserMapping, View,
};

/// Everything [`deserialize_schema`] recovers from a stored table schema.
pub type DecodedSchema = (
    u32,
    Vec<Column>,
    TableOptions,
    Option<ForeignTableMeta>,
    Vec<CheckConstraint>,
);

/// The single schema-value format version. All tables (ordinary and foreign)
/// are written with this version byte; a flag byte after the column list
/// distinguishes ordinary (`0`) from foreign (`1`), and a `CHECK` constraint
/// list closes the record.
pub const SCHEMA_VERSION: u8 = 7;

const TABLE_OPTION_SHARDED: u8 = 0b0000_0001;
const SHARDING_VERSION: u8 = 1;
const SHARDING_NONE: u8 = 0;
const SHARDING_HASH: u8 = 1;
const INDEX_VERSION: u8 = 4;
const SEQUENCE_VERSION: u8 = 1;
const INDEX_PLACEMENT_LOCAL: u8 = 0;
const INDEX_PLACEMENT_GLOBAL: u8 = 1;
const INDEX_METHOD_BTREE: u8 = 0;
const INDEX_METHOD_GIN: u8 = 1;
const INDEX_METHOD_HASH: u8 = 2;
const INDEX_METHOD_GIST: u8 = 3;
const INDEX_METHOD_SPGIST: u8 = 4;
const INDEX_CONSTRAINT_NONE: u8 = 0;
const INDEX_CONSTRAINT_PRIMARY_KEY: u8 = 1;
const INDEX_CONSTRAINT_UNIQUE: u8 = 2;
const INDEX_CONSTRAINT_EXCLUSION: u8 = 3;
const EXCLUSION_OPERATOR_EQUAL: u8 = 0;
const EXCLUSION_OPERATOR_OVERLAPS: u8 = 1;
const FOREIGN_KEY_VERSION: u8 = 1;
const REFERENTIAL_ACTION_NO_ACTION: u8 = 0;
const REFERENTIAL_ACTION_RESTRICT: u8 = 1;
const REFERENTIAL_ACTION_CASCADE: u8 = 2;
const REFERENTIAL_ACTION_SET_NULL: u8 = 3;
const REFERENTIAL_ACTION_SET_DEFAULT: u8 = 4;
const MATCH_TYPE_SIMPLE: u8 = 0;
const MATCH_TYPE_FULL: u8 = 1;

/// Tags for a persisted column DEFAULT value. Like [`type_tag`], this space is
/// **append-only**: a new value type takes the next free code and an existing
/// code never changes meaning, so adding one needs no [`SCHEMA_VERSION`] bump.
mod datum_tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
    pub const FLOAT8: u8 = 5;
    pub const NUMERIC: u8 = 6;
    /// `jsonb` — followed by the value's canonical text (u32 length + bytes),
    /// which is reparsed on read. Append-only — no version bump.
    pub const JSONB: u8 = 7;
    /// A one-dimensional array — followed by the element type's
    /// `ElemType::code()` byte, then the elements as a `crabka_pgkv::rowenc`
    /// row (u32 length + bytes). Reusing the row encoder keeps a second full
    /// datum encoder out of the catalog and covers every element type,
    /// including NULL and nested `jsonb`, for free. Append-only — no version
    /// bump.
    pub const ARRAY: u8 = 8;
    /// `smallint`. Append-only — no version bump.
    pub const INT2: u8 = 9;
    /// `real` — stored as the IEEE-754 bit pattern, like [`FLOAT8`].
    /// Append-only — no version bump.
    pub const FLOAT4: u8 = 10;
    /// `regclass` — followed by the relation's four-byte oid, and only the oid.
    /// The name `regclassout` prints is derived from the catalog when the
    /// default is read, never stored, so a default follows a `RENAME` of the
    /// relation it names and falls back to the bare oid once that relation is
    /// dropped — what `PostgreSQL` does with the oid its folded `Const` holds.
    /// Append-only — no version bump.
    pub const REGCLASS: u8 = 11;
    pub const TSVECTOR: u8 = 12;
    pub const TSQUERY: u8 = 13;
    pub const RANGE: u8 = 14;
    pub const MULTIRANGE: u8 = 15;
}

mod type_tag {
    pub const BOOL: u8 = 0;
    pub const INT4: u8 = 1;
    pub const INT8: u8 = 2;
    pub const TEXT: u8 = 3;
    /// SP30: `float8` / `double precision`. Append-only — no version bump.
    pub const FLOAT8: u8 = 4;
    /// SP32: `numeric` — followed by a typmod byte (0 = unconstrained; 1 = a
    /// `(precision: u16, scale: u16)` modifier). Append-only.
    pub const NUMERIC: u8 = 5;
    /// SP37: `date`. Append-only — no version bump.
    pub const DATE: u8 = 6;
    /// SP37: `time without time zone` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const TIME: u8 = 7;
    /// SP37: `timestamp without time zone` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const TIMESTAMP: u8 = 8;
    /// SP37: `timestamp with time zone` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const TIMESTAMPTZ: u8 = 9;
    /// SP37: `interval` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const INTERVAL: u8 = 10;
    /// SP40: `bytea`. Append-only — no version bump.
    pub const BYTEA: u8 = 11;
    pub const VARCHAR: u8 = 12;
    pub const BPCHAR: u8 = 13;
    pub const UUID: u8 = 14;
    /// `regclass` — a relation oid stored as `Int4`. Append-only — no version bump.
    pub const REGCLASS: u8 = 15;
    /// `jsonb`. Append-only — no version bump.
    pub const JSONB: u8 = 16;
    /// A one-dimensional array — followed by the element type's
    /// `ElemType::code()` byte. Append-only — no version bump.
    pub const ARRAY: u8 = 17;
    /// `smallint` / `int2`. Append-only — no version bump.
    pub const INT2: u8 = 18;
    /// `real` / `float4`. Append-only — no version bump.
    pub const FLOAT4: u8 = 19;
    /// `time with time zone` / `timetz` — followed by a reserved precision
    /// byte (0), like the other date/time tags. Append-only — no version bump.
    pub const TIMETZ: u8 = 20;
    /// A user-defined type — a composite, an enum or a domain — followed by its
    /// `pg_type.oid` as a big-endian `u32`. The definition lives in the type
    /// catalog, so the column stores only the identity. Append-only.
    pub const USER: u8 = 21;
    pub const TSVECTOR: u8 = 22;
    pub const TSQUERY: u8 = 23;
    /// `point`. Append-only — no version bump.
    pub const POINT: u8 = 24;
    /// `path`. Append-only — no version bump.
    pub const PATH: u8 = 25;
    /// PostgreSQL `oidvector`. Append-only — no version bump.
    pub const OIDVECTOR: u8 = 26;
    /// PostgreSQL `regtype`. Append-only — no version bump.
    pub const REGTYPE: u8 = 27;
    /// PostgreSQL `regprocedure`. Append-only — no version bump.
    pub const REGPROCEDURE: u8 = 28;
}

/// Append a column's type (tag byte, plus the numeric typmod payload).
pub(crate) fn write_type(out: &mut Vec<u8>, ty: ColumnType) {
    match ty {
        ColumnType::Bool => out.push(type_tag::BOOL),
        ColumnType::Int2 => out.push(type_tag::INT2),
        ColumnType::Int4 => out.push(type_tag::INT4),
        ColumnType::Int8 => out.push(type_tag::INT8),
        ColumnType::Text => out.push(type_tag::TEXT),
        ColumnType::Varchar(limit) => write_optional_u16_type(out, type_tag::VARCHAR, limit),
        ColumnType::Char(limit) => write_optional_u16_type(out, type_tag::BPCHAR, limit),
        ColumnType::Float4 => out.push(type_tag::FLOAT4),
        ColumnType::Float8 => out.push(type_tag::FLOAT8),
        ColumnType::Point => out.push(type_tag::POINT),
        ColumnType::Path => out.push(type_tag::PATH),
        ColumnType::Numeric(tm) => {
            out.push(type_tag::NUMERIC);
            match tm {
                Some(t) => {
                    out.push(1);
                    out.extend_from_slice(&t.precision.to_be_bytes());
                    out.extend_from_slice(&t.scale.to_be_bytes());
                }
                None => out.push(0),
            }
        }
        ColumnType::Date => out.push(type_tag::DATE),
        ColumnType::Time => {
            out.push(type_tag::TIME);
            out.push(0);
        }
        ColumnType::Timetz => {
            out.push(type_tag::TIMETZ);
            out.push(0);
        }
        ColumnType::Timestamp => {
            out.push(type_tag::TIMESTAMP);
            out.push(0);
        }
        ColumnType::Timestamptz => {
            out.push(type_tag::TIMESTAMPTZ);
            out.push(0);
        }
        ColumnType::Interval => {
            out.push(type_tag::INTERVAL);
            out.push(0);
        }
        ColumnType::Bytea => out.push(type_tag::BYTEA),
        ColumnType::Uuid => out.push(type_tag::UUID),
        ColumnType::Regclass => out.push(type_tag::REGCLASS),
        ColumnType::Regtype => out.push(type_tag::REGTYPE),
        ColumnType::Regprocedure => out.push(type_tag::REGPROCEDURE),
        ColumnType::OidVector => out.push(type_tag::OIDVECTOR),
        ColumnType::TsVector => out.push(type_tag::TSVECTOR),
        ColumnType::TsQuery => out.push(type_tag::TSQUERY),
        ColumnType::Jsonb => out.push(type_tag::JSONB),
        ColumnType::Array(elem) => {
            out.push(type_tag::ARRAY);
            // `write_code`, not `code()`: the bare code byte loses the length
            // modifier of a `varchar(n)`/`char(n)` element, which would silently
            // turn a `varchar(3)[]` column into an unbounded `varchar[]`.
            elem.write_code(out);
        }
        // A user-defined type is stored by oid; the definition is the type
        // catalog's. The anonymous `record` is a pseudo-type and is refused as
        // a column type before it reaches here, so its oid stands for "no
        // registered type" and fails the read back.
        ColumnType::Record(named) => {
            out.push(type_tag::USER);
            out.extend_from_slice(
                &named
                    .map_or(crabka_pgtypes::oids::RECORD, |ty| ty.oid)
                    .to_be_bytes(),
            );
        }
        ColumnType::Enum(named) => {
            out.push(type_tag::USER);
            out.extend_from_slice(&named.oid.to_be_bytes());
        }
        ColumnType::Range(range) => {
            out.push(type_tag::USER);
            out.extend_from_slice(&range.oid.to_be_bytes());
        }
        ColumnType::Multirange(multirange) => {
            out.push(type_tag::USER);
            out.extend_from_slice(&multirange.oid.to_be_bytes());
        }
        ColumnType::Domain(domain) => {
            out.push(type_tag::USER);
            out.extend_from_slice(&domain.oid.to_be_bytes());
        }
    }
}

/// Read a column's type, consuming the tag (and the numeric typmod payload).
pub(crate) fn read_type(cur: &mut &[u8]) -> Result<ColumnType, KvError> {
    Ok(match take_u8(cur)? {
        type_tag::BOOL => ColumnType::Bool,
        type_tag::INT2 => ColumnType::Int2,
        type_tag::INT4 => ColumnType::Int4,
        type_tag::INT8 => ColumnType::Int8,
        type_tag::TEXT => ColumnType::Text,
        type_tag::VARCHAR => ColumnType::Varchar(read_optional_u16_type(cur)?),
        type_tag::BPCHAR => ColumnType::Char(read_optional_u16_type(cur)?),
        type_tag::FLOAT4 => ColumnType::Float4,
        type_tag::FLOAT8 => ColumnType::Float8,
        type_tag::POINT => ColumnType::Point,
        type_tag::PATH => ColumnType::Path,
        type_tag::NUMERIC => {
            if take_u8(cur)? == 1 {
                let precision = u16::from_be_bytes(take_n(cur, 2)?.try_into().expect("2"));
                let scale = u16::from_be_bytes(take_n(cur, 2)?.try_into().expect("2"));
                ColumnType::Numeric(Some(Typmod { precision, scale }))
            } else {
                ColumnType::Numeric(None)
            }
        }
        type_tag::DATE => ColumnType::Date,
        type_tag::TIME => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Time
        }
        type_tag::TIMETZ => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Timetz
        }
        type_tag::TIMESTAMP => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Timestamp
        }
        type_tag::TIMESTAMPTZ => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Timestamptz
        }
        type_tag::INTERVAL => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Interval
        }
        type_tag::BYTEA => ColumnType::Bytea,
        type_tag::UUID => ColumnType::Uuid,
        type_tag::REGCLASS => ColumnType::Regclass,
        type_tag::REGTYPE => ColumnType::Regtype,
        type_tag::REGPROCEDURE => ColumnType::Regprocedure,
        type_tag::OIDVECTOR => ColumnType::OidVector,
        type_tag::TSVECTOR => ColumnType::TsVector,
        type_tag::TSQUERY => ColumnType::TsQuery,
        type_tag::JSONB => ColumnType::Jsonb,
        type_tag::ARRAY => {
            let elem = crabka_pgtypes::ElemType::read_code(cur)
                .ok_or_else(|| KvError::CorruptRow("unknown array element type encoding".into()))?;
            ColumnType::Array(elem)
        }
        type_tag::USER => {
            let raw = take_n(cur, 4)?;
            let oid = u32::from_be_bytes(raw.try_into().expect("4 bytes fit u32"));
            if let Some(builtin) = crabka_pgtypes::ColumnType::builtin_range(oid)
                .or_else(|| crabka_pgtypes::ColumnType::builtin_multirange(oid))
            {
                builtin
            } else {
                crabka_pgtypes::usertype::column_type_for_oid(oid).ok_or_else(|| {
                    KvError::CorruptRow(format!("column type oid {oid} is not a registered type"))
                })?
            }
        }
        other => {
            return Err(KvError::CorruptRow(format!(
                "unknown column type tag {other}"
            )));
        }
    })
}

fn write_optional_u16_type(out: &mut Vec<u8>, tag: u8, value: Option<u16>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn read_optional_u16_type(cur: &mut &[u8]) -> Result<Option<u16>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(u16::from_be_bytes(
            take_n(cur, 2)?.try_into().expect("2"),
        ))),
        flag => Err(KvError::CorruptRow(format!(
            "unknown string typmod flag {flag}"
        ))),
    }
}

fn write_default(out: &mut Vec<u8>, default: Option<&ColumnDefault>) {
    let Some(default) = default else {
        out.push(0);
        return;
    };
    match default {
        ColumnDefault::Value(value) => {
            out.push(1);
            write_default_value(out, value);
        }
        ColumnDefault::NextVal(sequence) => {
            out.push(2);
            write_str(out, sequence);
        }
    }
}

fn write_default_value(out: &mut Vec<u8>, default: &Datum) {
    match default {
        Datum::Null => out.push(datum_tag::NULL),
        Datum::Bool(value) => {
            out.push(datum_tag::BOOL);
            out.push(u8::from(*value));
        }
        Datum::Int2(value) => {
            out.push(datum_tag::INT2);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Datum::Int4(value) => {
            out.push(datum_tag::INT4);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Datum::Int8(value) => {
            out.push(datum_tag::INT8);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Datum::Text(value) => {
            out.push(datum_tag::TEXT);
            write_str(out, value);
        }
        Datum::Float4(value) => {
            out.push(datum_tag::FLOAT4);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Datum::Float8(value) => {
            out.push(datum_tag::FLOAT8);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Datum::Numeric(value) => {
            out.push(datum_tag::NUMERIC);
            write_str(out, &value.to_string());
        }
        Datum::Jsonb(value) => {
            out.push(datum_tag::JSONB);
            write_str(out, &value.to_text());
        }
        Datum::Array(array) => {
            out.push(datum_tag::ARRAY);
            array.elem.write_code(out);
            write_bytes(out, &crabka_pgkv::rowenc::encode_row(&array.elems));
        }
        // Only the oid: the relation name is re-derived on read.
        Datum::Regclass(value) => {
            out.push(datum_tag::REGCLASS);
            out.extend_from_slice(&value.oid.to_be_bytes());
        }
        Datum::TsVector(value) => {
            out.push(datum_tag::TSVECTOR);
            write_str(out, &value.to_string());
        }
        Datum::TsQuery(value) => {
            out.push(datum_tag::TSQUERY);
            write_str(out, &value.to_string());
        }
        Datum::Range(_) => {
            out.push(datum_tag::RANGE);
            write_bytes(
                out,
                &crabka_pgkv::rowenc::encode_row(std::slice::from_ref(default)),
            );
        }
        Datum::Multirange(_) => {
            out.push(datum_tag::MULTIRANGE);
            write_bytes(
                out,
                &crabka_pgkv::rowenc::encode_row(std::slice::from_ref(default)),
            );
        }
        Datum::Date(_)
        | Datum::Point(_)
        | Datum::Path(_)
        | Datum::Time(_)
        | Datum::Timetz(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_)
        | Datum::Record(_)
        | Datum::Enum(_)
        | Datum::OidVector(_)
        | Datum::Bytea(_) => {
            unreachable!("unsupported defaults are rejected before catalog write")
        }
    }
}

fn read_default(cur: &mut &[u8]) -> Result<Option<ColumnDefault>, KvError> {
    Ok(match take_u8(cur)? {
        0 => None,
        1 => Some(ColumnDefault::Value(read_default_value(cur)?)),
        2 => Some(ColumnDefault::NextVal(read_string(cur)?)),
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown column default flag {flag}"
            )));
        }
    })
}

fn read_default_value(cur: &mut &[u8]) -> Result<Datum, KvError> {
    let value = match take_u8(cur)? {
        datum_tag::NULL => Datum::Null,
        datum_tag::BOOL => Datum::Bool(match take_u8(cur)? {
            0 => false,
            1 => true,
            flag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown bool default flag {flag}"
                )));
            }
        }),
        datum_tag::INT2 => Datum::Int2(i16::from_be_bytes(take_n(cur, 2)?.try_into().expect("2"))),
        datum_tag::INT4 => Datum::Int4(i32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4"))),
        datum_tag::INT8 => Datum::Int8(i64::from_be_bytes(take_n(cur, 8)?.try_into().expect("8"))),
        datum_tag::TEXT => Datum::Text(read_string(cur)?),
        datum_tag::FLOAT4 => {
            let bits = u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4"));
            Datum::Float4(f32::from_bits(bits))
        }
        datum_tag::FLOAT8 => {
            let bits = u64::from_be_bytes(take_n(cur, 8)?.try_into().expect("8"));
            Datum::Float8(f64::from_bits(bits))
        }
        datum_tag::NUMERIC => {
            let raw = read_string(cur)?;
            Datum::Numeric(
                crabka_pgtypes::numeric::parse(&raw).ok_or_else(|| {
                    KvError::CorruptRow(format!("invalid numeric default {raw:?}"))
                })?,
            )
        }
        datum_tag::JSONB => {
            let raw = read_string(cur)?;
            Datum::Jsonb(
                crabka_pgtypes::jsonb::parse(&raw)
                    .map_err(|_| KvError::CorruptRow(format!("invalid jsonb default {raw:?}")))?,
            )
        }
        datum_tag::ARRAY => {
            let elem = crabka_pgtypes::ElemType::read_code(cur)
                .ok_or_else(|| KvError::CorruptRow("unknown array element type code".into()))?;
            let elems = crabka_pgkv::rowenc::decode_row(read_str(cur)?)?;
            Datum::Array(crabka_pgtypes::ArrayValue::new(elem, elems))
        }
        // Only the oid was stored, so the value comes back unresolved: the
        // catalog-aware layer above re-derives the name it prints.
        datum_tag::REGCLASS => Datum::Regclass(crabka_pgtypes::RegclassValue::unresolved(
            i32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")),
        )),
        datum_tag::TSVECTOR => {
            Datum::TsVector(read_string(cur)?.parse().map_err(|error| {
                KvError::CorruptRow(format!("invalid tsvector default: {error}"))
            })?)
        }
        datum_tag::TSQUERY => {
            Datum::TsQuery(read_string(cur)?.parse().map_err(|error| {
                KvError::CorruptRow(format!("invalid tsquery default: {error}"))
            })?)
        }
        datum_tag::RANGE => {
            let mut values = crabka_pgkv::rowenc::decode_row(read_str(cur)?)?;
            if values.len() != 1 || !matches!(values.first(), Some(Datum::Range(_))) {
                return Err(KvError::CorruptRow("invalid range default".into()));
            }
            values.pop().expect("length checked")
        }
        datum_tag::MULTIRANGE => {
            let mut values = crabka_pgkv::rowenc::decode_row(read_str(cur)?)?;
            if values.len() != 1 || !matches!(values.first(), Some(Datum::Multirange(_))) {
                return Err(KvError::CorruptRow("invalid multirange default".into()));
            }
            values.pop().expect("length checked")
        }
        tag => {
            return Err(KvError::CorruptRow(format!(
                "unknown default datum tag {tag}"
            )));
        }
    };
    Ok(value)
}

// ── Options helpers ───────────────────────────────────────────────────────────

/// Write a relation name as its two length-prefixed halves. A stored name is
/// never a dotted string, for the same reason a catalog key is not: the two
/// halves are not recoverable from one.
fn write_relation(out: &mut Vec<u8>, name: &crate::RelationName) {
    write_str(out, &name.schema);
    write_str(out, &name.name);
}

/// Read a relation name written by [`write_relation`].
fn read_relation(cur: &mut &[u8]) -> Result<crate::RelationName, KvError> {
    let schema = read_string(cur)?;
    let name = read_string(cur)?;
    Ok(crate::RelationName::new(schema, name))
}

pub(crate) fn write_str(out: &mut Vec<u8>, s: &str) {
    write_bytes(out, s.as_bytes());
}

/// Append a length-prefixed byte string (the framing `read_str` expects).
fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(
        &u32::try_from(bytes.len())
            .expect("catalog string length must fit in u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(bytes);
}

fn write_options(out: &mut Vec<u8>, opts: &[(String, String)]) {
    out.extend_from_slice(
        &u32::try_from(opts.len())
            .expect("catalog option count must fit in u32")
            .to_be_bytes(),
    );
    for (k, v) in opts {
        write_str(out, k);
        write_str(out, v);
    }
}

fn read_str<'a>(cur: &mut &'a [u8]) -> Result<&'a [u8], KvError> {
    let len = usize::try_from(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")))
        .expect("u32 fits in usize on supported targets");
    take_n(cur, len)
}

pub(crate) fn read_string(cur: &mut &[u8]) -> Result<String, KvError> {
    let bytes = read_str(cur)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| KvError::CorruptRow("non-UTF-8 string in catalog".into()))
}

fn read_options(cur: &mut &[u8]) -> Result<Vec<(String, String)>, KvError> {
    let n = usize::try_from(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")))
        .expect("u32 fits in usize on supported targets");
    let mut opts = Vec::with_capacity(n.min(256));
    for _ in 0..n {
        let k = read_string(cur)?;
        let v = read_string(cur)?;
        opts.push((k, v));
    }
    Ok(opts)
}

// ── Table schema ──────────────────────────────────────────────────────────────

/// Serialize a table schema (ordinary or foreign).
///
/// Always writes version byte `5`, then the column list, table option flags, and
/// a foreign flag: `0` for an ordinary table, `1` for a foreign table followed
/// by the foreign metadata payload.
///
/// # Panics
///
/// Panics when a catalog collection or string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_schema(
    table_id: u32,
    columns: &[Column],
    options: TableOptions,
    meta: Option<&ForeignTableMeta>,
    checks: &[CheckConstraint],
) -> Vec<u8> {
    let mut out = vec![SCHEMA_VERSION];
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(
        &u32::try_from(columns.len())
            .expect("catalog column count must fit in u32")
            .to_be_bytes(),
    );
    for c in columns {
        write_str(&mut out, &c.name);
        write_type(&mut out, c.ty);
        out.push(u8::from(c.not_null));
        write_default(&mut out, c.default.as_ref());
        write_generated(&mut out, c.generated.as_deref());
        out.push(identity_flag(c.identity));
    }
    out.push(table_option_flags(options));
    match meta {
        None => out.push(0),
        Some(m) => {
            out.push(1);
            write_str(&mut out, &m.server);
            write_options(&mut out, &m.options);
        }
    }
    write_checks(&mut out, checks);
    out
}

/// `GENERATED ALWAYS AS (expr) STORED`: a present/absent flag byte, then the
/// expression source when present.
fn write_generated(out: &mut Vec<u8>, generated: Option<&str>) {
    match generated {
        None => out.push(0),
        Some(expr) => {
            out.push(1);
            write_str(out, expr);
        }
    }
}

fn read_generated(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(cur)?)),
        flag => Err(KvError::CorruptRow(format!(
            "unknown generated-column flag {flag}"
        ))),
    }
}

const IDENTITY_NONE: u8 = 0;
const IDENTITY_ALWAYS: u8 = 1;
const IDENTITY_BY_DEFAULT: u8 = 2;

fn identity_flag(identity: Option<IdentityKind>) -> u8 {
    match identity {
        None => IDENTITY_NONE,
        Some(IdentityKind::Always) => IDENTITY_ALWAYS,
        Some(IdentityKind::ByDefault) => IDENTITY_BY_DEFAULT,
    }
}

fn read_identity(cur: &mut &[u8]) -> Result<Option<IdentityKind>, KvError> {
    match take_u8(cur)? {
        IDENTITY_NONE => Ok(None),
        IDENTITY_ALWAYS => Ok(Some(IdentityKind::Always)),
        IDENTITY_BY_DEFAULT => Ok(Some(IdentityKind::ByDefault)),
        flag => Err(KvError::CorruptRow(format!("unknown identity flag {flag}"))),
    }
}

/// A `CHECK` constraint list: a `u32` count, then each constraint's name,
/// predicate source, and `pg_constraint.convalidated` flag.
fn write_checks(out: &mut Vec<u8>, checks: &[CheckConstraint]) {
    out.extend_from_slice(
        &u32::try_from(checks.len())
            .expect("catalog check-constraint count must fit in u32")
            .to_be_bytes(),
    );
    for check in checks {
        write_str(out, &check.name);
        write_str(out, &check.expr);
        out.push(u8::from(check.validated));
    }
}

fn read_checks(cur: &mut &[u8]) -> Result<Vec<CheckConstraint>, KvError> {
    let count = usize::try_from(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")))
        .expect("u32 fits in usize on supported targets");
    let mut checks = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let name = read_string(cur)?;
        let expr = read_string(cur)?;
        let validated = match take_u8(cur)? {
            0 => false,
            1 => true,
            flag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown check-constraint validated flag {flag}"
                )));
            }
        };
        checks.push(CheckConstraint {
            name,
            expr,
            validated,
        });
    }
    Ok(checks)
}

fn table_option_flags(options: TableOptions) -> u8 {
    if options.sharded {
        TABLE_OPTION_SHARDED
    } else {
        0
    }
}

fn read_table_options(flags: u8) -> Result<TableOptions, KvError> {
    if flags & !TABLE_OPTION_SHARDED != 0 {
        return Err(KvError::CorruptRow(format!(
            "unknown table option flags {flags:#04x}"
        )));
    }
    Ok(TableOptions {
        sharded: flags & TABLE_OPTION_SHARDED != 0,
    })
}

/// Serialize a table's sharding strategy.
///
/// # Panics
///
/// Panics when the sharding column list or a string exceeds its `u32` wire
/// limit.
#[must_use]
pub fn serialize_sharding(sharding: Option<&ShardingStrategy>) -> Vec<u8> {
    let mut out = vec![SHARDING_VERSION];
    let Some(ShardingStrategy::Hash(hash)) = sharding else {
        out.push(SHARDING_NONE);
        return out;
    };

    out.push(SHARDING_HASH);
    out.extend_from_slice(
        &u32::try_from(hash.columns.len())
            .expect("hash sharding column count must fit in u32")
            .to_be_bytes(),
    );
    for column in &hash.columns {
        write_str(&mut out, column);
    }
    out.extend_from_slice(&hash.buckets.to_be_bytes());
    match &hash.co_location_group {
        None => out.push(0),
        Some(group) => {
            out.push(1);
            write_str(&mut out, group);
        }
    }
    out
}

/// Deserialize a table's sharding strategy.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated, unsupported, or invalid
/// sharding bytes.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array or `usize`.
pub fn deserialize_sharding(bytes: &[u8]) -> Result<Option<ShardingStrategy>, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != SHARDING_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unsupported sharding metadata version {version}"
        )));
    }
    let decoded = match take_u8(&mut cur)? {
        SHARDING_NONE => None,
        SHARDING_HASH => {
            let column_count = usize::try_from(u32::from_be_bytes(
                take_n(&mut cur, 4)?.try_into().expect("4"),
            ))
            .expect("u32 fits in usize on supported targets");
            if column_count != 1 {
                return Err(KvError::CorruptRow(
                    "hash sharding requires exactly one column".into(),
                ));
            }
            let mut columns = Vec::with_capacity(column_count.min(16));
            for _ in 0..column_count {
                let column = read_string(&mut cur)?;
                if column.is_empty() {
                    return Err(KvError::CorruptRow(
                        "hash sharding column name must not be empty".into(),
                    ));
                }
                columns.push(column);
            }
            let buckets = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            if buckets == 0 || !buckets.is_power_of_two() {
                return Err(KvError::CorruptRow(
                    "hash sharding bucket count must be a power of two".into(),
                ));
            }
            let co_location_group = match take_u8(&mut cur)? {
                0 => None,
                1 => {
                    let group = read_string(&mut cur)?;
                    if group.is_empty() {
                        return Err(KvError::CorruptRow(
                            "co-location group name must not be empty".into(),
                        ));
                    }
                    Some(group)
                }
                flag => {
                    return Err(KvError::CorruptRow(format!(
                        "unknown co-location group flag {flag}"
                    )));
                }
            };
            Some(ShardingStrategy::Hash(HashSharding {
                columns,
                buckets,
                co_location_group,
            }))
        }
        tag => {
            return Err(KvError::CorruptRow(format!(
                "unknown sharding strategy tag {tag}"
            )));
        }
    };
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "trailing bytes in sharding metadata".into(),
        ));
    }
    Ok(decoded)
}

/// Serialize an index catalog record.
///
/// # Panics
///
/// Panics when the index column list or a string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_index(index: &Index) -> Vec<u8> {
    let mut out = vec![INDEX_VERSION];
    out.extend_from_slice(&index.id.to_be_bytes());
    write_str(&mut out, &index.name);
    out.extend_from_slice(&index.table_id.to_be_bytes());
    write_relation(&mut out, &index.table);
    out.push(u8::from(index.unique));
    out.push(match index.placement {
        IndexPlacement::Local => INDEX_PLACEMENT_LOCAL,
        IndexPlacement::Global => INDEX_PLACEMENT_GLOBAL,
    });
    out.push(match index.method {
        IndexMethod::Btree => INDEX_METHOD_BTREE,
        IndexMethod::Gin => INDEX_METHOD_GIN,
        IndexMethod::Hash => INDEX_METHOD_HASH,
        IndexMethod::Gist => INDEX_METHOD_GIST,
        IndexMethod::Spgist => INDEX_METHOD_SPGIST,
    });
    out.push(match &index.constraint {
        None => INDEX_CONSTRAINT_NONE,
        Some(IndexConstraint::PrimaryKey) => INDEX_CONSTRAINT_PRIMARY_KEY,
        Some(IndexConstraint::Unique) => INDEX_CONSTRAINT_UNIQUE,
        Some(IndexConstraint::Exclusion(_)) => INDEX_CONSTRAINT_EXCLUSION,
    });
    if let Some(IndexConstraint::Exclusion(operators)) = &index.constraint {
        out.extend_from_slice(
            &u32::try_from(operators.len())
                .expect("exclusion operator count must fit in u32")
                .to_be_bytes(),
        );
        for operator in operators {
            out.push(match operator {
                ExclusionOperator::Equal => EXCLUSION_OPERATOR_EQUAL,
                ExclusionOperator::Overlaps => EXCLUSION_OPERATOR_OVERLAPS,
            });
        }
    }
    out.extend_from_slice(
        &u32::try_from(index.columns.len())
            .expect("index column count must fit in u32")
            .to_be_bytes(),
    );
    for column in &index.columns {
        write_str(&mut out, column);
    }
    out
}

/// Deserialize an index catalog record.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated or invalid record bytes.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array or `usize`.
pub fn deserialize_index(bytes: &[u8]) -> Result<Index, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if !(2..=INDEX_VERSION).contains(&version) {
        return Err(KvError::CorruptRow(format!(
            "unknown index version {version}"
        )));
    }
    let id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let name = read_string(&mut cur)?;
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let table = read_relation(&mut cur)?;
    let unique = match take_u8(&mut cur)? {
        0 => false,
        1 => true,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown index unique flag {flag}"
            )));
        }
    };
    let placement = match take_u8(&mut cur)? {
        INDEX_PLACEMENT_LOCAL => IndexPlacement::Local,
        INDEX_PLACEMENT_GLOBAL => IndexPlacement::Global,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown index placement flag {flag}"
            )));
        }
    };
    let method = if version >= 3 {
        match take_u8(&mut cur)? {
            INDEX_METHOD_BTREE => IndexMethod::Btree,
            INDEX_METHOD_GIN => IndexMethod::Gin,
            INDEX_METHOD_HASH => IndexMethod::Hash,
            INDEX_METHOD_GIST => IndexMethod::Gist,
            INDEX_METHOD_SPGIST => IndexMethod::Spgist,
            tag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown index method tag {tag}"
                )));
            }
        }
    } else {
        IndexMethod::Btree
    };
    let constraint = match take_u8(&mut cur)? {
        INDEX_CONSTRAINT_NONE => None,
        INDEX_CONSTRAINT_PRIMARY_KEY => Some(IndexConstraint::PrimaryKey),
        INDEX_CONSTRAINT_UNIQUE => Some(IndexConstraint::Unique),
        INDEX_CONSTRAINT_EXCLUSION if version >= 4 => {
            let count = usize::try_from(u32::from_be_bytes(
                take_n(&mut cur, 4)?.try_into().expect("4"),
            ))
            .expect("u32 fits in usize on supported targets");
            let mut operators = Vec::with_capacity(count.min(16));
            for _ in 0..count {
                operators.push(match take_u8(&mut cur)? {
                    EXCLUSION_OPERATOR_EQUAL => ExclusionOperator::Equal,
                    EXCLUSION_OPERATOR_OVERLAPS => ExclusionOperator::Overlaps,
                    tag => {
                        return Err(KvError::CorruptRow(format!(
                            "unknown exclusion operator tag {tag}"
                        )));
                    }
                });
            }
            Some(IndexConstraint::Exclusion(operators))
        }
        tag => {
            return Err(KvError::CorruptRow(format!(
                "unknown index constraint tag {tag}"
            )));
        }
    };
    let column_count = usize::try_from(u32::from_be_bytes(
        take_n(&mut cur, 4)?.try_into().expect("4"),
    ))
    .expect("u32 fits in usize on supported targets");
    if column_count == 0 {
        return Err(KvError::CorruptRow(
            "index requires at least one column".into(),
        ));
    }
    let mut columns = Vec::with_capacity(column_count.min(16));
    for _ in 0..column_count {
        columns.push(read_string(&mut cur)?);
    }
    if let Some(IndexConstraint::Exclusion(operators)) = &constraint
        && operators.len() != columns.len()
    {
        return Err(KvError::CorruptRow(
            "exclusion operator count does not match index column count".into(),
        ));
    }
    Ok(Index {
        id,
        name,
        table,
        table_id,
        columns,
        unique,
        placement,
        method,
        constraint,
    })
}

// ── Foreign keys ──────────────────────────────────────────────────────────────

/// Serialize a foreign-key constraint record.
///
/// Version byte, then the constraint's own creation-order id and its name; the
/// child relation's display name and its id; the referenced relation's display
/// name and its id; the referenced unique index's display name and its id; the
/// match type, `ON DELETE` and `ON UPDATE` actions as one byte each; the
/// deferrable, initially-deferred and validated flags as one byte each; and
/// finally the referencing, referenced and `SET NULL`/`SET DEFAULT` column-name
/// lists, each a `u32` count followed by that many length-prefixed names.
///
/// The ids are the authority — the display names are denormalized copies that a
/// rename rewrites — so the record is self-describing without a second lookup.
///
/// # Panics
///
/// Panics when a column list or a string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_foreign_key(fk: &ForeignKey) -> Vec<u8> {
    let mut out = vec![FOREIGN_KEY_VERSION];
    out.extend_from_slice(&fk.id.to_be_bytes());
    write_str(&mut out, &fk.name);
    write_relation(&mut out, &fk.table);
    out.extend_from_slice(&fk.table_id.to_be_bytes());
    write_relation(&mut out, &fk.referenced_table);
    out.extend_from_slice(&fk.referenced_table_id.to_be_bytes());
    write_str(&mut out, &fk.referenced_index);
    out.extend_from_slice(&fk.referenced_index_id.to_be_bytes());
    out.push(match fk.match_type {
        MatchType::Simple => MATCH_TYPE_SIMPLE,
        MatchType::Full => MATCH_TYPE_FULL,
    });
    out.push(referential_action_tag(fk.on_delete));
    out.push(referential_action_tag(fk.on_update));
    out.push(u8::from(fk.deferrable));
    out.push(u8::from(fk.initially_deferred));
    out.push(u8::from(fk.validated));
    write_string_list(&mut out, &fk.columns);
    write_string_list(&mut out, &fk.referenced_columns);
    write_string_list(&mut out, &fk.set_columns);
    out
}

/// Deserialize a foreign-key constraint record.
///
/// # Errors
///
/// Returns catalog corruption errors for a wrong version byte, truncated or
/// trailing bytes, an unknown enum or flag byte, an empty referencing column
/// list, or referencing and referenced column lists of different lengths.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array or `usize`.
pub fn deserialize_foreign_key(bytes: &[u8]) -> Result<ForeignKey, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != FOREIGN_KEY_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown foreign key version {version}"
        )));
    }
    let id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let name = read_string(&mut cur)?;
    let table = read_relation(&mut cur)?;
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let referenced_table = read_relation(&mut cur)?;
    let referenced_table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let referenced_index = read_string(&mut cur)?;
    let referenced_index_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let match_type = match take_u8(&mut cur)? {
        MATCH_TYPE_SIMPLE => MatchType::Simple,
        MATCH_TYPE_FULL => MatchType::Full,
        tag => {
            return Err(KvError::CorruptRow(format!(
                "unknown foreign key match type tag {tag}"
            )));
        }
    };
    let on_delete = read_referential_action(&mut cur)?;
    let on_update = read_referential_action(&mut cur)?;
    let deferrable = read_foreign_key_flag(&mut cur, "deferrable")?;
    let initially_deferred = read_foreign_key_flag(&mut cur, "initially deferred")?;
    let validated = read_foreign_key_flag(&mut cur, "validated")?;
    let columns = read_string_list(&mut cur)?;
    if columns.is_empty() {
        return Err(KvError::CorruptRow(
            "foreign key requires at least one column".into(),
        ));
    }
    let referenced_columns = read_string_list(&mut cur)?;
    if referenced_columns.len() != columns.len() {
        return Err(KvError::CorruptRow(format!(
            "foreign key references {} columns with {} referencing columns",
            referenced_columns.len(),
            columns.len()
        )));
    }
    let set_columns = read_string_list(&mut cur)?;
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "trailing bytes in foreign key record".into(),
        ));
    }
    Ok(ForeignKey {
        id,
        name,
        table,
        table_id,
        columns,
        referenced_table,
        referenced_table_id,
        referenced_columns,
        referenced_index_id,
        referenced_index,
        match_type,
        on_delete,
        on_update,
        set_columns,
        deferrable,
        initially_deferred,
        validated,
    })
}

fn referential_action_tag(action: ReferentialAction) -> u8 {
    match action {
        ReferentialAction::NoAction => REFERENTIAL_ACTION_NO_ACTION,
        ReferentialAction::Restrict => REFERENTIAL_ACTION_RESTRICT,
        ReferentialAction::Cascade => REFERENTIAL_ACTION_CASCADE,
        ReferentialAction::SetNull => REFERENTIAL_ACTION_SET_NULL,
        ReferentialAction::SetDefault => REFERENTIAL_ACTION_SET_DEFAULT,
    }
}

fn read_referential_action(cur: &mut &[u8]) -> Result<ReferentialAction, KvError> {
    match take_u8(cur)? {
        REFERENTIAL_ACTION_NO_ACTION => Ok(ReferentialAction::NoAction),
        REFERENTIAL_ACTION_RESTRICT => Ok(ReferentialAction::Restrict),
        REFERENTIAL_ACTION_CASCADE => Ok(ReferentialAction::Cascade),
        REFERENTIAL_ACTION_SET_NULL => Ok(ReferentialAction::SetNull),
        REFERENTIAL_ACTION_SET_DEFAULT => Ok(ReferentialAction::SetDefault),
        tag => Err(KvError::CorruptRow(format!(
            "unknown referential action tag {tag}"
        ))),
    }
}

fn read_foreign_key_flag(cur: &mut &[u8], what: &str) -> Result<bool, KvError> {
    match take_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(KvError::CorruptRow(format!(
            "unknown foreign key {what} flag {flag}"
        ))),
    }
}

/// Append a `u32` count followed by that many length-prefixed strings.
fn write_string_list(out: &mut Vec<u8>, values: &[String]) {
    out.extend_from_slice(
        &u32::try_from(values.len())
            .expect("catalog string list length must fit in u32")
            .to_be_bytes(),
    );
    for value in values {
        write_str(out, value);
    }
}

fn read_string_list(cur: &mut &[u8]) -> Result<Vec<String>, KvError> {
    let count = usize::try_from(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")))
        .expect("u32 fits in usize on supported targets");
    // The count is catalog-supplied, not client-supplied, but sizing the
    // allocation from it directly would still turn one corrupt byte into a
    // multi-gigabyte reservation; the pushes grow it for a genuinely long list.
    let mut values = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        values.push(read_string(cur)?);
    }
    Ok(values)
}

#[must_use]
pub fn serialize_sequence(sequence: Sequence) -> Vec<u8> {
    let mut out = vec![SEQUENCE_VERSION];
    for value in [
        sequence.start,
        sequence.increment,
        sequence.min,
        sequence.max,
        sequence.cache,
        sequence.last_value,
    ] {
        out.extend_from_slice(&value.to_be_bytes());
    }
    out.push(u8::from(sequence.cycle));
    out.push(u8::from(sequence.is_called));
    out
}

/// # Errors
///
/// Returns catalog corruption errors for truncated or invalid record bytes.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array.
pub fn deserialize_sequence(bytes: &[u8]) -> Result<Sequence, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != SEQUENCE_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown sequence version {version}"
        )));
    }
    let mut read_i64 = || -> Result<i64, KvError> {
        Ok(i64::from_be_bytes(
            take_n(&mut cur, 8)?.try_into().expect("8"),
        ))
    };
    let start = read_i64()?;
    let increment = read_i64()?;
    let min = read_i64()?;
    let max = read_i64()?;
    let cache = read_i64()?;
    let last_value = read_i64()?;
    let cycle = match take_u8(&mut cur)? {
        0 => false,
        1 => true,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown sequence cycle flag {flag}"
            )));
        }
    };
    let is_called = match take_u8(&mut cur)? {
        0 => false,
        1 => true,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown sequence called flag {flag}"
            )));
        }
    };
    Ok(Sequence {
        start,
        increment,
        min,
        max,
        cache,
        cycle,
        last_value,
        is_called,
    })
}

/// Deserialize a table schema.
///
/// Returns `(table_id, columns, table_options, Option<ForeignTableMeta>)`.
///
/// Returns `KvError::CorruptRow` if the version byte is not `5`, if the table
/// option flags contain unknown bits, or if the foreign flag after the option
/// flags is not `0` (ordinary) or `1` (foreign).
///
/// # Errors
///
/// Returns catalog corruption errors for truncated, unsupported, or invalid
/// schema bytes.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array or `usize`.
pub fn deserialize_schema(bytes: &[u8]) -> Result<DecodedSchema, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != SCHEMA_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown schema version {version}"
        )));
    }
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let ncols = usize::try_from(u32::from_be_bytes(
        take_n(&mut cur, 4)?.try_into().expect("4"),
    ))
    .expect("u32 fits in usize on supported targets");
    let mut columns = Vec::with_capacity(ncols.min(1024));
    for _ in 0..ncols {
        let name = read_string(&mut cur)?;
        let ty = read_type(&mut cur)?;
        let not_null = match take_u8(&mut cur)? {
            0 => false,
            1 => true,
            flag => return Err(KvError::CorruptRow(format!("unknown not-null flag {flag}"))),
        };
        let default = read_default(&mut cur)?;
        let generated = read_generated(&mut cur)?;
        let identity = read_identity(&mut cur)?;
        columns.push(Column {
            name,
            ty,
            not_null,
            default,
            generated,
            identity,
        });
    }
    let options = read_table_options(take_u8(&mut cur)?)?;
    let foreign = match take_u8(&mut cur)? {
        0 => None,
        1 => {
            let server = read_string(&mut cur)?;
            let options = read_options(&mut cur)?;
            Some(ForeignTableMeta { server, options })
        }
        flag => {
            return Err(KvError::CorruptRow(format!("unknown foreign flag {flag}")));
        }
    };
    let checks = read_checks(&mut cur)?;
    Ok((table_id, columns, options, foreign, checks))
}

// ── Foreign-data wrapper ──────────────────────────────────────────────────────

/// Format: `name len (u32) | name | options`.
#[must_use]
pub fn serialize_fdw(name: &str, options: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_str(&mut out, name);
    write_options(&mut out, options);
    out
}

/// Deserialize a foreign-data-wrapper record.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated or invalid record bytes.
pub fn deserialize_fdw(bytes: &[u8]) -> Result<ForeignDataWrapper, KvError> {
    let mut cur = bytes;
    let name = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(ForeignDataWrapper { name, options })
}

// ── User-defined types ────────────────────────────────────────────────────────

/// Serialize a user-defined type: `oid`, name, a kind byte, then the kind's own
/// payload (a composite's fields, an enum's labels, a domain's base type,
/// nullability, default and checks).
#[must_use]
pub fn serialize_user_type(ty: &UserType) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&ty.oid.to_be_bytes());
    write_str(&mut out, &ty.name);
    match &ty.body {
        UserTypeBody::Composite(fields) => {
            out.push(USER_TYPE_COMPOSITE);
            write_count(&mut out, fields.len());
            for field in fields {
                write_str(&mut out, &field.name);
                write_type(&mut out, field.ty);
            }
        }
        UserTypeBody::Enum(labels) => {
            out.push(USER_TYPE_ENUM);
            write_count(&mut out, labels.len());
            for label in labels {
                write_str(&mut out, label);
            }
        }
        UserTypeBody::Range(range) => {
            out.push(USER_TYPE_RANGE);
            write_type(&mut out, range.subtype);
            match &range.collation {
                Some(collation) => {
                    out.push(1);
                    write_str(&mut out, collation);
                }
                None => out.push(0),
            }
            match &range.multirange_name {
                Some(name) => {
                    out.push(1);
                    write_str(&mut out, name);
                }
                None => out.push(0),
            }
        }
        UserTypeBody::Domain(domain) => {
            out.push(USER_TYPE_DOMAIN);
            write_type(&mut out, domain.base);
            out.push(u8::from(domain.not_null));
            match &domain.default {
                Some(default) => {
                    out.push(1);
                    write_str(&mut out, default);
                }
                None => out.push(0),
            }
            write_count(&mut out, domain.checks.len());
            for check in &domain.checks {
                write_str(&mut out, &check.name);
                write_str(&mut out, &check.expr);
            }
        }
    }
    out
}

/// Deserialize a user-defined type.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated or invalid record bytes.
///
/// # Panics
///
/// If a fixed-width field's slice is not the width the reader just asked for,
/// which cannot happen: `take_n` either yields exactly that many bytes or
/// returns the corruption error above.
pub fn deserialize_user_type(bytes: &[u8]) -> Result<UserType, KvError> {
    let mut cur = bytes;
    let oid = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4 bytes fit u32"));
    let name = read_string(&mut cur)?;
    let body = match take_u8(&mut cur)? {
        USER_TYPE_COMPOSITE => {
            let count = read_count(&mut cur)?;
            let mut fields = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let field_name = read_string(&mut cur)?;
                fields.push(CompositeField {
                    name: field_name,
                    ty: read_type(&mut cur)?,
                });
            }
            UserTypeBody::Composite(fields)
        }
        USER_TYPE_ENUM => {
            let count = read_count(&mut cur)?;
            let mut labels = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                labels.push(read_string(&mut cur)?);
            }
            UserTypeBody::Enum(labels)
        }
        USER_TYPE_RANGE => {
            let subtype = read_type(&mut cur)?;
            let collation = match take_u8(&mut cur)? {
                0 => None,
                _ => Some(read_string(&mut cur)?),
            };
            let multirange_name = if cur.is_empty() || take_u8(&mut cur)? == 0 {
                None
            } else {
                Some(read_string(&mut cur)?)
            };
            UserTypeBody::Range(RangeBody {
                subtype,
                collation,
                multirange_name,
            })
        }
        USER_TYPE_DOMAIN => {
            let base = read_type(&mut cur)?;
            let not_null = take_u8(&mut cur)? != 0;
            let default = match take_u8(&mut cur)? {
                0 => None,
                _ => Some(read_string(&mut cur)?),
            };
            let count = read_count(&mut cur)?;
            let mut checks = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let check_name = read_string(&mut cur)?;
                checks.push(DomainCheck {
                    name: check_name,
                    expr: read_string(&mut cur)?,
                });
            }
            UserTypeBody::Domain(DomainBody {
                base,
                not_null,
                default,
                checks,
            })
        }
        other => {
            return Err(KvError::CorruptRow(format!(
                "unknown user type kind {other}"
            )));
        }
    };
    Ok(UserType { oid, name, body })
}

const USER_TYPE_COMPOSITE: u8 = 1;
const USER_TYPE_ENUM: u8 = 2;
const USER_TYPE_DOMAIN: u8 = 3;
const USER_TYPE_RANGE: u8 = 4;

fn write_count(out: &mut Vec<u8>, count: usize) {
    out.extend_from_slice(
        &u32::try_from(count)
            .expect("catalog element count must fit in u32")
            .to_be_bytes(),
    );
}

fn read_count(cur: &mut &[u8]) -> Result<usize, KvError> {
    Ok(
        usize::try_from(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")))
            .expect("u32 fits in usize on supported targets"),
    )
}

// ── Foreign server ────────────────────────────────────────────────────────────

/// Format: `name len | name | wrapper len | wrapper | options`.
#[must_use]
pub fn serialize_server(name: &str, wrapper: &str, options: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_str(&mut out, name);
    write_str(&mut out, wrapper);
    write_options(&mut out, options);
    out
}

/// Deserialize a foreign server record.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated or invalid record bytes.
pub fn deserialize_server(bytes: &[u8]) -> Result<ForeignServer, KvError> {
    let mut cur = bytes;
    let name = read_string(&mut cur)?;
    let wrapper = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(ForeignServer {
        name,
        wrapper,
        options,
    })
}

// ── User mapping ──────────────────────────────────────────────────────────────

/// Format: `user len | user | server len | server | options`.
#[must_use]
pub fn serialize_user_mapping(user: &str, server: &str, options: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_str(&mut out, user);
    write_str(&mut out, server);
    write_options(&mut out, options);
    out
}

/// Deserialize a user mapping record.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated or invalid record bytes.
pub fn deserialize_user_mapping(bytes: &[u8]) -> Result<UserMapping, KvError> {
    let mut cur = bytes;
    let user = read_string(&mut cur)?;
    let server = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(UserMapping {
        user,
        server,
        options,
    })
}

// ── Views ─────────────────────────────────────────────────────────────────────

const VIEW_VERSION: u8 = 1;

/// Serialize a view definition and its resolved output schema.
///
/// # Panics
///
/// Panics when the view column list or a string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_view(view: &View) -> Vec<u8> {
    let mut out = vec![VIEW_VERSION];
    write_relation(&mut out, &view.name);
    write_str(&mut out, &view.definition);
    out.extend_from_slice(
        &u32::try_from(view.columns.len())
            .expect("view column count must fit in u32")
            .to_be_bytes(),
    );
    for column in &view.columns {
        write_str(&mut out, &column.name);
        write_type(&mut out, column.ty);
    }
    out
}

/// Deserialize a view definition and its resolved output schema.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated, unsupported, or invalid view
/// bytes.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array or `usize`.
pub fn deserialize_view(bytes: &[u8]) -> Result<View, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != VIEW_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown view version {version}"
        )));
    }
    let name = read_relation(&mut cur)?;
    let definition = read_string(&mut cur)?;
    let column_count = usize::try_from(u32::from_be_bytes(
        take_n(&mut cur, 4)?.try_into().expect("4"),
    ))
    .expect("u32 fits in usize on supported targets");
    let mut columns = Vec::with_capacity(column_count.min(1024));
    for _ in 0..column_count {
        columns.push(Column {
            name: read_string(&mut cur)?,
            ty: read_type(&mut cur)?,
            not_null: false,
            default: None,
            generated: None,
            identity: None,
        });
    }
    if !cur.is_empty() {
        return Err(KvError::CorruptRow("trailing bytes in view record".into()));
    }
    Ok(View {
        name,
        definition,
        columns,
    })
}

// ── Shared primitives ─────────────────────────────────────────────────────────

pub(crate) fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (h, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated schema".into()))?;
    *cur = rest;
    Ok(*h)
}

pub(crate) fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated schema field".into()));
    }
    let (h, rest) = cur.split_at(n);
    *cur = rest;
    Ok(h)
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::{ColumnType, Datum};

    use super::*;
    use crate::{Column, ForeignTableMeta, RelationName};

    #[test]
    fn roundtrip_schema() {
        let table_id = 42u32;
        let columns = vec![
            Column::new("id", ColumnType::Int4),
            Column::new("name", ColumnType::Text),
            Column::new("ok", ColumnType::Bool),
            Column::new("big", ColumnType::Int8),
            Column::new("score", ColumnType::Float8),
            Column {
                name: "amount".into(),
                ty: ColumnType::Numeric(Some(crabka_pgtypes::numeric::Typmod {
                    precision: 10,
                    scale: 2,
                })),
                not_null: false,
                default: None,
                generated: None,
                identity: None,
            },
            Column {
                name: "ratio".into(),
                ty: ColumnType::Numeric(None),
                not_null: false,
                default: None,
                generated: None,
                identity: None,
            },
            Column::new("code", ColumnType::Varchar(Some(8))),
            Column::new("flag", ColumnType::Char(Some(2))),
            Column::new("public_id", ColumnType::Uuid),
        ];
        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None, &[]);
        let (id, cols, options, foreign, _) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(!options.sharded);
        assert!(foreign.is_none());
    }

    #[test]
    fn roundtrip_column_defaults_and_not_null() {
        let table_id = 12u32;
        let columns = vec![Column {
            name: "name".into(),
            ty: ColumnType::Text,
            not_null: true,
            default: Some(ColumnDefault::Value(Datum::Text("anon".into()))),
            generated: None,
            identity: None,
        }];

        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None, &[]);
        let (_id, decoded, _options, _foreign, _) = deserialize_schema(&bytes).expect("decode");

        assert_eq!(decoded, columns);
    }

    /// `jsonb` and array DEFAULT values survive the catalog round trip, including
    /// the awkward shapes: a nested object/array document, an array holding NULL
    /// elements, an empty array (whose element type lives only in the tag), and
    /// an array of `jsonb`.
    #[test]
    fn roundtrip_jsonb_and_array_column_defaults() {
        use assert2::assert;
        use crabka_pgtypes::{ArrayValue, ElemType};

        let doc = crabka_pgtypes::jsonb::parse(r#"{"b":[1,{"c":null}],"a":"x"}"#).expect("jsonb");
        let columns = vec![
            Column {
                name: "doc".into(),
                ty: ColumnType::Jsonb,
                not_null: false,
                default: Some(ColumnDefault::Value(Datum::Jsonb(doc.clone()))),
                generated: None,
                identity: None,
            },
            Column {
                name: "holes".into(),
                ty: ColumnType::Array(ElemType::Int4),
                not_null: false,
                default: Some(ColumnDefault::Value(Datum::Array(ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Null, Datum::Int4(3)],
                )))),
                generated: None,
                identity: None,
            },
            Column {
                name: "empty".into(),
                ty: ColumnType::Array(ElemType::Text),
                not_null: false,
                default: Some(ColumnDefault::Value(Datum::Array(ArrayValue::new(
                    ElemType::Text,
                    Vec::new(),
                )))),
                generated: None,
                identity: None,
            },
            Column {
                name: "docs".into(),
                ty: ColumnType::Array(ElemType::Jsonb),
                not_null: false,
                default: Some(ColumnDefault::Value(Datum::Array(ArrayValue::new(
                    ElemType::Jsonb,
                    vec![Datum::Jsonb(doc), Datum::Null],
                )))),
                generated: None,
                identity: None,
            },
        ];

        let bytes = serialize_schema(31, &columns, TableOptions::default(), None, &[]);
        let (_id, decoded, _options, _foreign, _) = deserialize_schema(&bytes).expect("decode");

        assert!(decoded == columns);
    }

    #[test]
    fn roundtrip_schema_datetime_types() {
        let table_id = 99u32;
        let columns = vec![
            Column::new("created", ColumnType::Date),
            Column::new("alarm", ColumnType::Time),
            Column::new("fired_at", ColumnType::Timestamp),
            Column::new("fired_utc", ColumnType::Timestamptz),
            Column::new("duration", ColumnType::Interval),
        ];
        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None, &[]);
        let (id, cols, options, foreign, _) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(!options.sharded);
        assert!(foreign.is_none());
    }

    /// Every `jsonb`/array column type survives a catalog round trip — the
    /// element code is what distinguishes one array column from another, so all
    /// of them are exercised, not just one.
    #[test]
    fn roundtrip_schema_jsonb_and_array_types() {
        use assert2::assert;
        use crabka_pgtypes::ElemType;

        let table_id = 21u32;
        let mut columns = vec![Column::new("doc", ColumnType::Jsonb)];
        for elem in ElemType::ALL {
            columns.push(Column::new(
                format!("arr_{}", elem.code()),
                ColumnType::Array(elem),
            ));
        }
        let range =
            match ColumnType::builtin_range(crabka_pgtypes::oids::INT8RANGE).expect("int8range") {
                ColumnType::Range(range) => range,
                _ => unreachable!(),
            };
        columns.push(Column::new(
            "ranges",
            ColumnType::Array(ElemType::Range(range)),
        ));
        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None, &[]);
        let (id, cols, _options, _foreign, _) = deserialize_schema(&bytes).expect("decode");
        assert!(id == table_id);
        assert!(cols == columns);
    }

    #[test]
    fn unknown_array_element_code_is_a_corrupt_row() {
        use assert2::assert;

        let columns = vec![Column::new(
            "arr",
            ColumnType::Array(crabka_pgtypes::ElemType::Int4),
        )];
        let mut bytes = serialize_schema(3, &columns, TableOptions::default(), None, &[]);
        // The element code is the byte after the ARRAY tag; corrupt it.
        let tag_at = bytes
            .iter()
            .position(|b| *b == type_tag::ARRAY)
            .expect("array tag");
        bytes[tag_at + 1] = 200;
        assert!(deserialize_schema(&bytes).is_err());
    }

    /// Every `ColumnType` must survive `write_type`/`read_type` unchanged.
    ///
    /// The tag table is hand-maintained, so a new type whose tag collides with
    /// an existing one, or whose read arm is missing, would otherwise surface as
    /// a column silently decoding to the wrong type rather than as a failure.
    /// This encoding was once reconstructed from its callers after an accidental
    /// revert, which is exactly the situation this test exists to catch.
    #[test]
    fn every_column_type_round_trips_through_its_tag() {
        let types = [
            ColumnType::Bool,
            ColumnType::Int2,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Text,
            ColumnType::Varchar(None),
            ColumnType::Varchar(Some(10)),
            ColumnType::Char(None),
            ColumnType::Char(Some(4)),
            ColumnType::Float4,
            ColumnType::Float8,
            ColumnType::Numeric(None),
            ColumnType::Date,
            ColumnType::Time,
            ColumnType::Timetz,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::Interval,
            ColumnType::Bytea,
            ColumnType::Uuid,
            ColumnType::Regclass,
            ColumnType::Jsonb,
            ColumnType::builtin_range(crabka_pgtypes::oids::INT4RANGE).expect("built-in range"),
        ];

        // Every element type has an array type, and the length-modified families
        // carry their modifier on the element — a `varchar(3)[]` column must not
        // read back as an unbounded `varchar[]`, which is what a bare element
        // code byte would give.
        let arrays = crabka_pgtypes::ElemType::ALL.into_iter().chain([
            crabka_pgtypes::ElemType::Varchar(Some(3)),
            crabka_pgtypes::ElemType::Char(Some(2)),
        ]);

        for ty in types.into_iter().chain(arrays.map(ColumnType::Array)) {
            let mut bytes = Vec::new();
            write_type(&mut bytes, ty);
            let mut cursor = bytes.as_slice();
            let decoded = read_type(&mut cursor).expect("every written type reads back");
            assert!(decoded == ty, "{ty:?} decoded as {decoded:?}");
            assert!(cursor.is_empty(), "{ty:?} left trailing bytes");
        }
    }

    /// No two `ColumnType` tags may collide — a collision makes one type decode
    /// as the other, which no single-type round trip can detect.
    #[test]
    fn column_type_tags_are_distinct() {
        let types = [
            ColumnType::Bool,
            ColumnType::Int2,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Text,
            ColumnType::Float4,
            ColumnType::Float8,
            ColumnType::Date,
            ColumnType::Time,
            ColumnType::Timetz,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::Interval,
            ColumnType::Bytea,
            ColumnType::Uuid,
            ColumnType::Regclass,
            ColumnType::Jsonb,
        ];

        let mut tags = std::collections::BTreeMap::new();
        for ty in types {
            let mut bytes = Vec::new();
            write_type(&mut bytes, ty);
            let tag = bytes[0];
            if let Some(previous) = tags.insert(tag, ty) {
                panic!("tag {tag} is shared by {previous:?} and {ty:?}");
            }
        }
    }

    #[test]
    fn roundtrip_foreign_table() {
        let table_id = 7u32;
        let columns = vec![
            Column::new("_partition", ColumnType::Int4),
            Column::new("_offset", ColumnType::Int8),
            Column::new("_timestamp", ColumnType::Timestamptz),
            Column::new("_key", ColumnType::Bytea),
            Column::new("_headers", ColumnType::Text),
            Column::new("payload", ColumnType::Text),
        ];
        let meta = ForeignTableMeta {
            server: "kafka_srv".into(),
            options: vec![("topic".into(), "events".into())],
        };
        let bytes = serialize_schema(
            table_id,
            &columns,
            TableOptions::default(),
            Some(&meta),
            &[],
        );
        let (id, cols, options, foreign, _) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(!options.sharded);
        let ft = foreign.expect("foreign meta round-trips");
        assert_eq!(ft.server, "kafka_srv");
        assert_eq!(ft.options, vec![("topic".into(), "events".into())]);
    }

    #[test]
    fn roundtrip_fdw() {
        let bytes = serialize_fdw(
            "kafka_fdw",
            &[("handler".into(), "kafka_fdw_handler".into())],
        );
        let fdw = deserialize_fdw(&bytes).expect("decode");
        assert_eq!(fdw.name, "kafka_fdw");
        assert_eq!(fdw.options[0].0, "handler");
    }

    #[test]
    fn roundtrip_server() {
        let bytes = serialize_server(
            "kafka_s",
            "kafka_fdw",
            &[("bootstrap".into(), "h:9092".into())],
        );
        let s = deserialize_server(&bytes).expect("decode");
        assert_eq!(s.name, "kafka_s");
        assert_eq!(s.wrapper, "kafka_fdw");
        assert_eq!(s.options[0], ("bootstrap".into(), "h:9092".into()));
    }

    #[test]
    fn roundtrip_user_mapping() {
        let bytes =
            serialize_user_mapping("alice", "kafka_s", &[("token".into(), "secret".into())]);
        let m = deserialize_user_mapping(&bytes).expect("decode");
        assert_eq!(m.user, "alice");
        assert_eq!(m.server, "kafka_s");
        assert_eq!(m.options[0], ("token".into(), "secret".into()));
    }

    #[test]
    fn roundtrip_range_type_metadata() {
        let ty = UserType {
            oid: 300_000,
            name: "textrange".into(),
            body: UserTypeBody::Range(RangeBody {
                subtype: ColumnType::Text,
                collation: Some("C".into()),
                multirange_name: Some("multirange_of_text".into()),
            }),
        };
        assert_eq!(deserialize_user_type(&serialize_user_type(&ty)), Ok(ty));
    }

    #[test]
    fn unknown_version_errors() {
        assert!(deserialize_schema(&[1, 0, 0, 0, 0]).is_err());
        assert!(deserialize_schema(&[99, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn ordinary_table_flag_zero_roundtrip() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(1, &columns, TableOptions::default(), None, &[]);
        let (_, _, options, foreign, _) =
            deserialize_schema(&bytes).expect("ordinary table decode");
        assert!(!options.sharded, "ordinary table has no sharded flag");
        assert!(foreign.is_none(), "ordinary table has no foreign meta");
    }

    #[test]
    fn sharded_option_roundtrips() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(1, &columns, TableOptions { sharded: true }, None, &[]);
        let (_, _, options, foreign, _) = deserialize_schema(&bytes).expect("sharded decode");
        assert!(options.sharded);
        assert!(foreign.is_none());
    }

    #[test]
    fn hash_sharding_option_roundtrips() {
        let sharding = ShardingStrategy::Hash(HashSharding {
            columns: vec!["x".into()],
            buckets: 16,
            co_location_group: Some("orders".into()),
        });
        let bytes = serialize_sharding(Some(&sharding));
        assert_eq!(
            deserialize_sharding(&bytes).expect("hash sharding decode"),
            Some(sharding)
        );
    }

    #[test]
    fn hash_sharding_decode_requires_exactly_one_column() {
        use assert2::assert;

        let hash_with = |columns: Vec<String>| {
            ShardingStrategy::Hash(HashSharding {
                columns,
                buckets: 16,
                co_location_group: None,
            })
        };

        // Zero and two columns are both rejected: `hash_bucket_for_row` hashes
        // only the first column, so anything but exactly one column would be
        // stored somewhere no routed lookup would ever probe.
        for columns in [Vec::new(), vec!["a".into(), "b".into()]] {
            let bytes = serialize_sharding(Some(&hash_with(columns)));
            assert!(deserialize_sharding(&bytes).is_err());
        }

        // A single column still round-trips.
        let single = hash_with(vec!["a".into()]);
        let bytes = serialize_sharding(Some(&single));
        assert!(deserialize_sharding(&bytes).expect("single-column decode") == Some(single));

        // A sharding-less table is unaffected by the hash-only arity check.
        let bytes = serialize_sharding(None);
        assert!(
            deserialize_sharding(&bytes)
                .expect("no-sharding decode")
                .is_none()
        );
    }

    #[test]
    fn hash_sharding_decode_rejects_trailing_bytes_and_empty_names() {
        let valid = ShardingStrategy::Hash(HashSharding {
            columns: vec!["id".into()],
            buckets: 16,
            co_location_group: None,
        });
        let mut trailing = serialize_sharding(Some(&valid));
        trailing.push(0);
        assert!(deserialize_sharding(&trailing).is_err());

        for invalid in [
            ShardingStrategy::Hash(HashSharding {
                columns: vec![String::new()],
                buckets: 16,
                co_location_group: None,
            }),
            ShardingStrategy::Hash(HashSharding {
                columns: vec!["id".into()],
                buckets: 16,
                co_location_group: Some(String::new()),
            }),
        ] {
            assert!(deserialize_sharding(&serialize_sharding(Some(&invalid))).is_err());
        }
    }

    #[test]
    fn index_record_roundtrips() {
        for method in [
            IndexMethod::Btree,
            IndexMethod::Hash,
            IndexMethod::Gist,
            IndexMethod::Gin,
            IndexMethod::Spgist,
        ] {
            let index = Index {
                id: 7,
                name: "orders_email_idx".into(),
                table: RelationName::public("orders"),
                table_id: 3,
                columns: vec!["email".into()],
                unique: true,
                placement: IndexPlacement::Global,
                method,
                constraint: None,
            };
            assert_eq!(
                deserialize_index(&serialize_index(&index)).expect("index decode"),
                index
            );
        }

        let exclusion = Index {
            id: 8,
            name: "booking_room_during_excl".into(),
            table: RelationName::public("booking"),
            table_id: 4,
            columns: vec!["room".into(), "during".into()],
            unique: false,
            placement: IndexPlacement::Local,
            method: IndexMethod::Gist,
            constraint: Some(IndexConstraint::Exclusion(vec![
                ExclusionOperator::Equal,
                ExclusionOperator::Overlaps,
            ])),
        };
        assert_eq!(
            deserialize_index(&serialize_index(&exclusion)).expect("exclusion index decode"),
            exclusion
        );
    }

    fn foreign_key_fixture() -> ForeignKey {
        ForeignKey {
            id: 4,
            name: "order_items_order_fkey".into(),
            table: RelationName::public("order_items"),
            table_id: 12,
            columns: vec!["order_id".into(), "order_line".into()],
            referenced_table: RelationName::public("orders"),
            referenced_table_id: 3,
            referenced_columns: vec!["id".into(), "line".into()],
            referenced_index_id: 9,
            referenced_index: "orders_pkey".into(),
            match_type: MatchType::Simple,
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            set_columns: Vec::new(),
            deferrable: false,
            initially_deferred: false,
            validated: true,
        }
    }

    /// Offset of the match-type byte, the first of the six single-byte fields
    /// that close the record's fixed head.
    fn match_type_offset(fk: &ForeignKey) -> usize {
        // A relation name is written as two length-prefixed parts.
        let relation = |name: &RelationName| (4 + name.schema.len()) + (4 + name.name.len());
        // Version byte, then the constraint's own id, then its name.
        1 + 4
            + (4 + fk.name.len())
            + relation(&fk.table)
            + 4
            + relation(&fk.referenced_table)
            + 4
            + (4 + fk.referenced_index.len())
            + 4
    }

    /// Every enum value, both match types, both `set_columns` shapes and all
    /// four flag combinations survive the round trip on a composite key.
    #[test]
    fn foreign_key_record_round_trips_every_action_match_and_flag() {
        use assert2::assert;

        let actions = [
            ReferentialAction::NoAction,
            ReferentialAction::Restrict,
            ReferentialAction::Cascade,
            ReferentialAction::SetNull,
            ReferentialAction::SetDefault,
        ];
        let mut cases = Vec::new();
        for on_delete in actions {
            for on_update in actions {
                for match_type in [MatchType::Simple, MatchType::Full] {
                    for set_columns in [Vec::new(), vec!["order_id".into()]] {
                        for (deferrable, initially_deferred) in
                            [(false, false), (true, false), (false, true), (true, true)]
                        {
                            for validated in [false, true] {
                                cases.push(ForeignKey {
                                    on_delete,
                                    on_update,
                                    match_type,
                                    set_columns: set_columns.clone(),
                                    deferrable,
                                    initially_deferred,
                                    validated,
                                    ..foreign_key_fixture()
                                });
                            }
                        }
                    }
                }
            }
        }

        for fk in cases {
            let decoded = deserialize_foreign_key(&serialize_foreign_key(&fk)).expect("decode");
            assert!(decoded == fk);
        }
    }

    #[test]
    fn foreign_key_decode_rejects_wrong_version_truncation_and_trailing_bytes() {
        use assert2::assert;

        let fk = foreign_key_fixture();
        let bytes = serialize_foreign_key(&fk);

        let mut wrong_version = bytes.clone();
        wrong_version[0] = FOREIGN_KEY_VERSION + 1;
        assert!(deserialize_foreign_key(&wrong_version).is_err());
        assert!(deserialize_foreign_key(&[]).is_err());

        for truncated in 1..bytes.len() {
            assert!(
                deserialize_foreign_key(&bytes[..truncated]).is_err(),
                "{truncated} bytes decoded as a whole record"
            );
        }

        let mut trailing = bytes;
        trailing.push(0);
        assert!(deserialize_foreign_key(&trailing).is_err());
    }

    #[test]
    fn foreign_key_decode_rejects_unknown_enum_and_flag_bytes() {
        use assert2::assert;

        let fk = foreign_key_fixture();
        let bytes = serialize_foreign_key(&fk);
        // Match type, ON DELETE, ON UPDATE, deferrable, initially deferred,
        // validated — in that order.
        for (offset, invalid) in (match_type_offset(&fk)..).zip([2, 5, 5, 2, 2, 2]) {
            let mut corrupt = bytes.clone();
            corrupt[offset] = invalid;
            assert!(
                deserialize_foreign_key(&corrupt).is_err(),
                "byte {offset} accepted the invalid value {invalid}"
            );
        }
    }

    /// The two column lists are the constraint's column pairing, so an empty
    /// referencing list or lists of different lengths are corruption, not a
    /// constraint over no columns.
    #[test]
    fn foreign_key_decode_rejects_unpaired_column_lists() {
        use assert2::assert;

        for broken in [
            ForeignKey {
                columns: Vec::new(),
                referenced_columns: Vec::new(),
                ..foreign_key_fixture()
            },
            ForeignKey {
                referenced_columns: vec!["id".into()],
                ..foreign_key_fixture()
            },
        ] {
            assert!(deserialize_foreign_key(&serialize_foreign_key(&broken)).is_err());
        }
    }

    #[test]
    fn unknown_flag_byte_errors() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let mut bytes = serialize_schema(1, &columns, TableOptions::default(), None, &[]);
        let last = bytes.last_mut().expect("foreign flag byte exists");
        *last = 2;
        assert!(deserialize_schema(&bytes).is_err());
    }

    #[test]
    fn unknown_table_option_flags_error() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let mut bytes = serialize_schema(1, &columns, TableOptions::default(), None, &[]);
        let option_flag_offset = bytes.len() - 2;
        bytes[option_flag_offset] = 0b1000_0000;
        assert!(deserialize_schema(&bytes).is_err());
    }

    #[test]
    fn truncated_errors_not_panics() {
        assert!(deserialize_schema(&[SCHEMA_VERSION, 0, 0]).is_err());
    }

    #[test]
    fn take_n_consumes_exactly_all_remaining() {
        let data = [1u8, 2, 3, 4];
        let mut cur: &[u8] = &data;
        assert_eq!(take_n(&mut cur, 4).expect("exact length is valid"), &data);
        assert!(cur.is_empty());
        let mut cur: &[u8] = &data;
        assert!(take_n(&mut cur, 5).is_err());
    }
}
