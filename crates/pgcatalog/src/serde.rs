//! Versioned (de)serialization of a table schema — the value stored under
//! `crabka_pgkv::key::catalog_key(name)`. Format: version byte, `table_id`
//! (u32 BE), column count (u32 BE), then per column: u32 name length, name bytes,
//! type tag; table option flags (u8: sharded, row security, forced row
//! security); the owning role (u32 length + name bytes);
//! followed by a `foreign` flag byte: `0` = ordinary table (no further payload),
//! `1` = foreign table (server name len u32, server name bytes, table option
//! list, then column-option entries, each with a column name and option list);
//! the `CHECK` constraint list; and a `materialized` flag byte: `0` = not a
//! materialized view (no further payload), `1` = materialized view (definition
//! len u32, definition bytes, `relispopulated` byte).
//!
//! Foreign-data-wrapper, foreign-server, and user-mapping objects use their own
//! simple binary format, not the schema format.

use crabka_pgkv::KvError;
use crabka_pgtypes::{
    ColumnType, Datum,
    numeric::Typmod,
    usertype::{
        BaseBody, CompositeField, DomainBody, DomainCheck, RangeBody, UserType, UserTypeBody,
    },
};

use crate::{
    CheckConstraint, Column, ColumnDefault, ConstraintDeferral, ExclusionOperator,
    ForeignDataWrapper, ForeignKey, ForeignServer, ForeignTableMeta, GeneratedColumn,
    GeneratedKind, HashSharding, IdentityKind, Index, IndexConstraint, IndexMethod, IndexPlacement,
    MatchType, MaterializedView, ReferentialAction, Sequence, ShardingStrategy, TableOptions,
    UserMapping, View, ViewCheckOption, ViewOptions,
};

/// Everything [`deserialize_schema`] recovers from a stored table schema:
/// `table_id`, columns, storage options, owning role, foreign metadata,
/// `CHECK` constraints, and materialized-view metadata.
pub type DecodedSchema = (
    u32,
    Vec<Column>,
    TableOptions,
    String,
    Option<ForeignTableMeta>,
    Vec<CheckConstraint>,
    Option<MaterializedView>,
);

/// The single schema-value format version. Every stored relation — ordinary,
/// foreign, or materialized view — is written with this version byte; a flag
/// byte after the owner distinguishes ordinary (`0`) from foreign (`1`), and a
/// `CHECK` constraint list and a materialized-view flag byte close the record.
pub const SCHEMA_VERSION: u8 = 31;

/// The `interval` type payload normally is one precision byte. This marker
/// introduces the packed field-range typmod that follows it.
const INTERVAL_RANGE_TYPMOD: u8 = u8::MAX - 1;

const TABLE_OPTION_SHARDED: u8 = 0b0000_0001;
const TABLE_OPTION_ROW_SECURITY: u8 = 0b0000_0010;
const TABLE_OPTION_FORCE_ROW_SECURITY: u8 = 0b0000_0100;
/// Every bit [`read_table_options`] knows how to decode. A bit outside this
/// mask is a record from a build this one does not understand, and the reader
/// refuses it rather than guessing — see the comment on `read_table_options`.
const TABLE_OPTION_KNOWN: u8 =
    TABLE_OPTION_SHARDED | TABLE_OPTION_ROW_SECURITY | TABLE_OPTION_FORCE_ROW_SECURITY;
const SHARDING_VERSION: u8 = 1;
const SHARDING_NONE: u8 = 0;
const SHARDING_HASH: u8 = 1;
const INDEX_VERSION: u8 = 12;
const INDEX_VERSION_KEY_OPTIONS: u8 = 10;
const INDEX_VERSION_INCLUDE: u8 = 9;
const INDEX_VERSION_PREDICATE: u8 = 8;
const INDEX_VERSION_LEGACY: u8 = 7;
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
const INDEX_DEFERRAL_IMMEDIATE: u8 = 0;
const INDEX_DEFERRAL_DEFERRABLE: u8 = 1;
const INDEX_DEFERRAL_DEFERRED: u8 = 2;
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

/// Tags for a persisted column DEFAULT value.
///
/// Like [`type_tag`], this space is **append-only**. A new value type takes the
/// next free code, and an existing code never changes meaning. A new tag
/// therefore needs no [`SCHEMA_VERSION`] bump.
mod datum_tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
    pub const FLOAT8: u8 = 5;
    pub const NUMERIC: u8 = 6;
    /// `jsonb`, followed by the value's canonical text as a u32 length and the
    /// bytes. The reader parses that text again. Append-only, with no version
    /// bump.
    pub const JSONB: u8 = 7;
    /// A one-dimensional array, followed by the element type's
    /// `ElemType::code()` byte. The elements then follow as a
    /// `crabka_pgkv::rowenc` row, a u32 length and the bytes. Reuse of the row
    /// encoder keeps a second full datum encoder out of the catalog, and covers
    /// every element type at no cost, NULL and nested `jsonb` included.
    /// Append-only, with no version bump.
    pub const ARRAY: u8 = 8;
    /// `smallint`. Append-only, with no version bump.
    pub const INT2: u8 = 9;
    /// `real`, stored as the IEEE-754 bit pattern, like [`FLOAT8`].
    /// Append-only, with no version bump.
    pub const FLOAT4: u8 = 10;
    /// `regclass`, followed by the relation's four-byte oid, and only the oid.
    /// The reader derives the name `regclassout` prints from the catalog when
    /// it reads the default, and never stores that name. A default therefore
    /// follows a `RENAME` of the relation it names, and falls back to the bare
    /// oid once that relation is dropped. `PostgreSQL` does the same with the
    /// oid its folded `Const` holds. Append-only, with no version bump.
    pub const REGCLASS: u8 = 11;
    pub const TSVECTOR: u8 = 12;
    pub const TSQUERY: u8 = 13;
    pub const RANGE: u8 = 14;
    pub const MULTIRANGE: u8 = 15;
    pub const JSONPATH: u8 = 16;
    /// A network address — `inet`, `cidr`, `macaddr` or `macaddr8` — stored as
    /// a one-column `crabka_pgkv::rowenc` row (u32 length + bytes), which
    /// already tags the variant and holds the `is_cidr` flag. Append-only — no
    /// version bump.
    pub const NETWORK: u8 = 17;
    /// A `bit` / `bit varying` value, stored as a one-column
    /// `crabka_pgkv::rowenc` row, which already carries the bit count and the
    /// `varying` flag. Append-only — no version bump.
    pub const BITSTRING: u8 = 18;
    /// A `money` value, stored as its `i64` minor-unit count. Append-only — no
    /// version bump.
    pub const MONEY: u8 = 19;
    /// `json` — followed by the input text (u32 length + bytes), stored and
    /// returned verbatim. Distinct from [`JSONB`] because a `jsonb` round trip
    /// would normalise it. Append-only — no version bump.
    pub const JSON: u8 = 20;
    /// A system identifier value — `oid`, `xid`, `xid8`, `cid`, `tid` or
    /// `pg_lsn` — stored as a one-column `crabka_pgkv::rowenc` row, which
    /// already tags which of the six it is. Append-only — no version bump.
    pub const SYSID: u8 = 21;
    /// `xml` — followed by the document text (u32 length + bytes), stored and
    /// returned verbatim, exactly as [`JSON`] is. Append-only — no version bump.
    pub const XML: u8 = 22;
    /// A `"char"` value, stored as the byte itself. Not [`TEXT`]: the escaped
    /// `\ooo` spelling is the type's text form, not its value, and the high
    /// half of its range has no text form that is valid UTF-8. Append-only —
    /// no version bump.
    pub const INTERNAL_CHAR: u8 = 23;
    /// A `pg_snapshot` or `txid_snapshot` value — followed by its canonical
    /// `xmin:xmax:xip` text (u32 length + bytes), the way [`TSVECTOR`] and
    /// [`TSQUERY`] store theirs. The text form is the canonical form here, so
    /// nothing is lost by it. Append-only — no version bump.
    pub const PG_SNAPSHOT: u8 = 24;
}

mod type_tag {
    pub const BOOL: u8 = 0;
    pub const INT4: u8 = 1;
    pub const INT8: u8 = 2;
    pub const TEXT: u8 = 3;
    /// SP30: `float8` / `double precision`. Append-only, with no version bump.
    pub const FLOAT8: u8 = 4;
    /// SP32: `numeric`, followed by a typmod byte. A `0` byte is unconstrained
    /// and a `1` byte is a `(precision: u16, scale: u16)` modifier.
    /// Append-only.
    pub const NUMERIC: u8 = 5;
    /// SP37: `date`. Append-only, with no version bump.
    pub const DATE: u8 = 6;
    /// SP37: `time without time zone`, followed by a reserved precision byte
    /// (0). Append-only, with no version bump.
    pub const TIME: u8 = 7;
    /// SP37: `timestamp without time zone`, followed by a reserved precision
    /// byte (0). Append-only, with no version bump.
    pub const TIMESTAMP: u8 = 8;
    /// SP37: `timestamp with time zone`, followed by a reserved precision byte
    /// (0). Append-only, with no version bump.
    pub const TIMESTAMPTZ: u8 = 9;
    /// SP37: `interval`, followed by a reserved precision byte (0).
    /// Append-only, with no version bump.
    pub const INTERVAL: u8 = 10;
    /// SP40: `bytea`. Append-only, with no version bump.
    pub const BYTEA: u8 = 11;
    pub const VARCHAR: u8 = 12;
    pub const BPCHAR: u8 = 13;
    pub const UUID: u8 = 14;
    /// `regclass`, a relation oid stored as `Int4`. Append-only, with no
    /// version bump.
    pub const REGCLASS: u8 = 15;
    /// `jsonb`. Append-only, with no version bump.
    pub const JSONB: u8 = 16;
    /// A one-dimensional array, followed by the element type's
    /// `ElemType::code()` byte. Append-only, with no version bump.
    pub const ARRAY: u8 = 17;
    /// `smallint` / `int2`. Append-only, with no version bump.
    pub const INT2: u8 = 18;
    /// `real` / `float4`. Append-only, with no version bump.
    pub const FLOAT4: u8 = 19;
    /// `time with time zone` / `timetz`, followed by a reserved precision byte
    /// (0), like the other date/time tags. Append-only, with no version bump.
    pub const TIMETZ: u8 = 20;
    /// A user-defined type: a composite, an enum or a domain. Its
    /// `pg_type.oid` follows as a big-endian `u32`. The definition lives in the
    /// type catalog, so the column stores only the identity. Append-only.
    pub const USER: u8 = 21;
    pub const TSVECTOR: u8 = 22;
    pub const TSQUERY: u8 = 23;
    /// `point`. Append-only — no version bump.
    pub const POINT: u8 = 24;
    /// `path`. Append-only — no version bump.
    pub const PATH: u8 = 25;
    /// `PostgreSQL` `oidvector`. Append-only — no version bump.
    pub const OIDVECTOR: u8 = 26;
    /// `PostgreSQL` `regtype`. Append-only — no version bump.
    pub const REGTYPE: u8 = 27;
    /// `PostgreSQL` `regprocedure`. Append-only — no version bump.
    pub const REGPROCEDURE: u8 = 28;
    /// `PostgreSQL` `jsonpath`. Append-only — no version bump.
    pub const JSONPATH: u8 = 29;
    /// `PostgreSQL` `int2vector`. Append-only — no version bump.
    pub const INT2VECTOR: u8 = 30;
    /// `PostgreSQL` `lseg`. Append-only — no version bump.
    pub const LSEG: u8 = 31;
    /// `PostgreSQL` `line`. Append-only — no version bump.
    pub const LINE: u8 = 32;
    /// `PostgreSQL` `circle`. Append-only — no version bump.
    pub const CIRCLE: u8 = 33;
    /// `PostgreSQL` `box`. Append-only — no version bump.
    pub const BOX: u8 = 34;
    /// `PostgreSQL` `regnamespace`. Append-only — no version bump.
    pub const REGNAMESPACE: u8 = 35;
    /// `PostgreSQL` `inet`. Append-only — no version bump.
    pub const INET: u8 = 36;
    /// `PostgreSQL` `cidr`. Append-only — no version bump.
    pub const CIDR: u8 = 37;
    /// `PostgreSQL` `macaddr`. Append-only — no version bump.
    pub const MACADDR: u8 = 38;
    /// `PostgreSQL` `macaddr8`. Append-only — no version bump.
    pub const MACADDR8: u8 = 39;
    /// `PostgreSQL` `bit`, with its optional length modifier. Append-only — no
    /// version bump.
    pub const BIT: u8 = 40;
    /// `PostgreSQL` `bit varying`, with its optional length modifier.
    /// Append-only — no version bump.
    pub const VARBIT: u8 = 41;
    /// `PostgreSQL` `money`. Append-only — no version bump.
    pub const MONEY: u8 = 42;
    /// `json`. Append-only — no version bump.
    pub const JSON: u8 = 43;
    /// `PostgreSQL` `oid`. Append-only — no version bump.
    pub const OID: u8 = 44;
    /// `PostgreSQL` `xid`. Append-only — no version bump.
    pub const XID: u8 = 45;
    /// `PostgreSQL` `xid8`. Append-only — no version bump.
    pub const XID8: u8 = 46;
    /// `PostgreSQL` `cid`. Append-only — no version bump.
    pub const CID: u8 = 47;
    /// `PostgreSQL` `tid`. Append-only — no version bump.
    pub const TID: u8 = 48;
    /// `PostgreSQL` `pg_lsn`. Append-only — no version bump.
    pub const PG_LSN: u8 = 49;
    /// `PostgreSQL` `regproc`. Append-only — no version bump.
    pub const REGPROC: u8 = 50;
    /// `PostgreSQL` `regoper`. Append-only — no version bump.
    pub const REGOPER: u8 = 51;
    /// `PostgreSQL` `regoperator`. Append-only — no version bump.
    pub const REGOPERATOR: u8 = 52;
    /// `PostgreSQL` `regconfig`. Append-only — no version bump.
    pub const REGCONFIG: u8 = 53;
    /// `PostgreSQL` `regdictionary`. Append-only — no version bump.
    pub const REGDICTIONARY: u8 = 54;
    /// `PostgreSQL` `regrole`. Append-only — no version bump.
    pub const REGROLE: u8 = 55;
    /// `PostgreSQL` `regcollation`. Append-only — no version bump.
    pub const REGCOLLATION: u8 = 56;
    /// `xml`. Append-only — no version bump.
    pub const XML: u8 = 57;
    /// `PostgreSQL` `polygon`. Append-only — no version bump.
    pub const POLYGON: u8 = 58;
    /// `PostgreSQL` `"char"`, the one-byte type — not [`BPCHAR`], which is
    /// `character(n)`. Append-only — no version bump.
    pub const INTERNAL_CHAR: u8 = 59;
    /// `PostgreSQL` `pg_snapshot`. Append-only — no version bump.
    pub const PG_SNAPSHOT: u8 = 60;
    /// `PostgreSQL` `txid_snapshot`. Its own tag rather than
    /// [`PG_SNAPSHOT`]'s, because the two are different SQL types at different
    /// oids: a column declared `txid_snapshot` must still report 2970 after a
    /// restart, which only a tag of its own preserves. Append-only — no
    /// version bump.
    pub const TXID_SNAPSHOT: u8 = 61;
}

#[derive(Debug)]
pub(crate) enum UserTypeDecodeError {
    Corrupt(KvError),
    UnresolvedUserType(u32),
}

impl UserTypeDecodeError {
    pub(crate) fn into_kv_error(self) -> KvError {
        match self {
            Self::Corrupt(error) => error,
            Self::UnresolvedUserType(oid) => {
                KvError::CorruptRow(format!("column type oid {oid} is not a registered type"))
            }
        }
    }
}

impl From<KvError> for UserTypeDecodeError {
    fn from(error: KvError) -> Self {
        Self::Corrupt(error)
    }
}

/// Append a column's type (tag byte, plus the numeric typmod payload).
pub(crate) fn write_type(out: &mut Vec<u8>, ty: ColumnType) {
    match ty {
        ColumnType::Bool => out.push(type_tag::BOOL),
        ColumnType::Int2 => out.push(type_tag::INT2),
        ColumnType::Int4 => out.push(type_tag::INT4),
        ColumnType::Int8 => out.push(type_tag::INT8),
        ColumnType::Text => out.push(type_tag::TEXT),
        ColumnType::Name => {
            out.push(type_tag::USER);
            out.extend_from_slice(&crabka_pgtypes::oids::NAME.to_be_bytes());
        }
        ColumnType::Aclitem => {
            out.push(type_tag::USER);
            out.extend_from_slice(&crabka_pgtypes::oids::ACLITEM.to_be_bytes());
        }
        ColumnType::Refcursor => {
            out.push(type_tag::USER);
            out.extend_from_slice(&crabka_pgtypes::oids::REFCURSOR.to_be_bytes());
        }
        ColumnType::Varchar(limit) => write_optional_u16_type(out, type_tag::VARCHAR, limit),
        ColumnType::Char(limit) => write_optional_u16_type(out, type_tag::BPCHAR, limit),
        ColumnType::Float4 => out.push(type_tag::FLOAT4),
        ColumnType::Float8 => out.push(type_tag::FLOAT8),
        ColumnType::Point => out.push(type_tag::POINT),
        ColumnType::Path => out.push(type_tag::PATH),
        ColumnType::Polygon => out.push(type_tag::POLYGON),
        ColumnType::Lseg => out.push(type_tag::LSEG),
        ColumnType::Line => out.push(type_tag::LINE),
        ColumnType::Circle => out.push(type_tag::CIRCLE),
        ColumnType::Box => out.push(type_tag::BOX),
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
            out.push(u8::MAX);
        }
        ColumnType::Timetz => {
            out.push(type_tag::TIMETZ);
            out.push(u8::MAX);
        }
        ColumnType::Timestamp => {
            out.push(type_tag::TIMESTAMP);
            out.push(u8::MAX);
        }
        ColumnType::Timestamptz => {
            out.push(type_tag::TIMESTAMPTZ);
            out.push(u8::MAX);
        }
        ColumnType::Interval => {
            out.push(type_tag::INTERVAL);
            out.push(u8::MAX);
        }
        ColumnType::Temporal(kind, precision) => {
            out.push(match kind {
                crabka_pgtypes::TemporalType::Time => type_tag::TIME,
                crabka_pgtypes::TemporalType::Timetz => type_tag::TIMETZ,
                crabka_pgtypes::TemporalType::Timestamp => type_tag::TIMESTAMP,
                crabka_pgtypes::TemporalType::Timestamptz => type_tag::TIMESTAMPTZ,
                crabka_pgtypes::TemporalType::Interval => type_tag::INTERVAL,
            });
            out.push(precision);
        }
        ColumnType::IntervalTypmod(typmod) => {
            out.push(type_tag::INTERVAL);
            out.push(INTERVAL_RANGE_TYPMOD);
            out.extend_from_slice(&typmod.typmod().to_be_bytes());
        }
        ColumnType::Bytea => out.push(type_tag::BYTEA),
        ColumnType::Uuid => out.push(type_tag::UUID),
        ColumnType::Regclass => out.push(type_tag::REGCLASS),
        ColumnType::Regtype => out.push(type_tag::REGTYPE),
        ColumnType::Regprocedure => out.push(type_tag::REGPROCEDURE),
        ColumnType::Regnamespace => out.push(type_tag::REGNAMESPACE),
        ColumnType::Regproc => out.push(type_tag::REGPROC),
        ColumnType::Regoper => out.push(type_tag::REGOPER),
        ColumnType::Regoperator => out.push(type_tag::REGOPERATOR),
        ColumnType::Regconfig => out.push(type_tag::REGCONFIG),
        ColumnType::Regdictionary => out.push(type_tag::REGDICTIONARY),
        ColumnType::Regrole => out.push(type_tag::REGROLE),
        ColumnType::Regcollation => out.push(type_tag::REGCOLLATION),
        ColumnType::OidVector => out.push(type_tag::OIDVECTOR),
        ColumnType::Int2Vector => out.push(type_tag::INT2VECTOR),
        ColumnType::TsVector => out.push(type_tag::TSVECTOR),
        ColumnType::TsQuery => out.push(type_tag::TSQUERY),
        ColumnType::Inet => out.push(type_tag::INET),
        ColumnType::Cidr => out.push(type_tag::CIDR),
        ColumnType::MacAddr => out.push(type_tag::MACADDR),
        ColumnType::MacAddr8 => out.push(type_tag::MACADDR8),
        ColumnType::Money => out.push(type_tag::MONEY),
        ColumnType::InternalChar => out.push(type_tag::INTERNAL_CHAR),
        ColumnType::Oid => out.push(type_tag::OID),
        ColumnType::Xid => out.push(type_tag::XID),
        ColumnType::Xid8 => out.push(type_tag::XID8),
        ColumnType::Cid => out.push(type_tag::CID),
        ColumnType::Tid => out.push(type_tag::TID),
        ColumnType::PgLsn => out.push(type_tag::PG_LSN),
        ColumnType::PgSnapshot => out.push(type_tag::PG_SNAPSHOT),
        ColumnType::TxidSnapshot => out.push(type_tag::TXID_SNAPSHOT),
        ColumnType::Bit(len) => write_optional_i32_type(out, type_tag::BIT, len),
        ColumnType::VarBit(len) => write_optional_i32_type(out, type_tag::VARBIT, len),
        ColumnType::Json => out.push(type_tag::JSON),
        ColumnType::Xml => out.push(type_tag::XML),
        ColumnType::Jsonb => out.push(type_tag::JSONB),
        ColumnType::JsonPath => out.push(type_tag::JSONPATH),
        ColumnType::Array(elem) => {
            out.push(type_tag::ARRAY);
            // `write_code`, not `code()`: the bare code byte loses the length
            // modifier of a `varchar(n)`/`char(n)` element, which would silently
            // turn a `varchar(3)[]` column into an unbounded `varchar[]`.
            elem.write_code(out);
        }
        // A user-defined type is stored by oid; the definition is the type
        // catalog's. The anonymous `record` pseudo-type uses its built-in oid.
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
        ColumnType::Base(base) => {
            out.push(type_tag::USER);
            out.extend_from_slice(&base.oid.to_be_bytes());
        }
    }
}

/// Read a column's type, consuming the tag (and the numeric typmod payload).
pub(crate) fn read_type(cur: &mut &[u8]) -> Result<ColumnType, KvError> {
    read_type_with(cur, &crabka_pgtypes::usertype::column_type_for_oid)
        .map_err(UserTypeDecodeError::into_kv_error)
}

fn read_type_with(
    cur: &mut &[u8],
    resolve_user_type: &dyn Fn(u32) -> Option<ColumnType>,
) -> Result<ColumnType, UserTypeDecodeError> {
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
        type_tag::POLYGON => ColumnType::Polygon,
        type_tag::LSEG => ColumnType::Lseg,
        type_tag::LINE => ColumnType::Line,
        type_tag::CIRCLE => ColumnType::Circle,
        type_tag::BOX => ColumnType::Box,
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
            let precision = take_u8(cur)?;
            if precision != u8::MAX && precision > 6 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()).into());
            }
            if precision == u8::MAX {
                ColumnType::Time
            } else {
                ColumnType::Temporal(crabka_pgtypes::TemporalType::Time, precision)
            }
        }
        type_tag::TIMETZ => {
            let precision = take_u8(cur)?;
            if precision != u8::MAX && precision > 6 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()).into());
            }
            if precision == u8::MAX {
                ColumnType::Timetz
            } else {
                ColumnType::Temporal(crabka_pgtypes::TemporalType::Timetz, precision)
            }
        }
        type_tag::TIMESTAMP => {
            let precision = take_u8(cur)?;
            if precision != u8::MAX && precision > 6 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()).into());
            }
            if precision == u8::MAX {
                ColumnType::Timestamp
            } else {
                ColumnType::Temporal(crabka_pgtypes::TemporalType::Timestamp, precision)
            }
        }
        type_tag::TIMESTAMPTZ => {
            let precision = take_u8(cur)?;
            if precision != u8::MAX && precision > 6 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()).into());
            }
            if precision == u8::MAX {
                ColumnType::Timestamptz
            } else {
                ColumnType::Temporal(crabka_pgtypes::TemporalType::Timestamptz, precision)
            }
        }
        type_tag::INTERVAL => {
            let precision = take_u8(cur)?;
            if precision == INTERVAL_RANGE_TYPMOD {
                let packed = i32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4"));
                let typmod =
                    crabka_pgtypes::IntervalTypmod::from_typmod(packed).ok_or_else(|| {
                        KvError::CorruptRow("unsupported interval field range".into())
                    })?;
                return Ok(ColumnType::IntervalTypmod(typmod));
            }
            if precision != u8::MAX && precision > 6 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()).into());
            }
            if precision == u8::MAX {
                ColumnType::Interval
            } else {
                ColumnType::Temporal(crabka_pgtypes::TemporalType::Interval, precision)
            }
        }
        type_tag::BYTEA => ColumnType::Bytea,
        type_tag::UUID => ColumnType::Uuid,
        type_tag::REGCLASS => ColumnType::Regclass,
        type_tag::REGTYPE => ColumnType::Regtype,
        type_tag::REGPROCEDURE => ColumnType::Regprocedure,
        type_tag::REGNAMESPACE => ColumnType::Regnamespace,
        type_tag::REGPROC => ColumnType::Regproc,
        type_tag::REGOPER => ColumnType::Regoper,
        type_tag::REGOPERATOR => ColumnType::Regoperator,
        type_tag::REGCONFIG => ColumnType::Regconfig,
        type_tag::REGDICTIONARY => ColumnType::Regdictionary,
        type_tag::REGROLE => ColumnType::Regrole,
        type_tag::REGCOLLATION => ColumnType::Regcollation,
        type_tag::OIDVECTOR => ColumnType::OidVector,
        type_tag::INT2VECTOR => ColumnType::Int2Vector,
        type_tag::TSVECTOR => ColumnType::TsVector,
        type_tag::TSQUERY => ColumnType::TsQuery,
        type_tag::INET => ColumnType::Inet,
        type_tag::CIDR => ColumnType::Cidr,
        type_tag::MACADDR => ColumnType::MacAddr,
        type_tag::MACADDR8 => ColumnType::MacAddr8,
        type_tag::MONEY => ColumnType::Money,
        type_tag::INTERNAL_CHAR => ColumnType::InternalChar,
        type_tag::OID => ColumnType::Oid,
        type_tag::XID => ColumnType::Xid,
        type_tag::XID8 => ColumnType::Xid8,
        type_tag::CID => ColumnType::Cid,
        type_tag::TID => ColumnType::Tid,
        type_tag::PG_LSN => ColumnType::PgLsn,
        type_tag::PG_SNAPSHOT => ColumnType::PgSnapshot,
        type_tag::TXID_SNAPSHOT => ColumnType::TxidSnapshot,
        type_tag::BIT => ColumnType::Bit(read_optional_i32_type(cur)?),
        type_tag::VARBIT => ColumnType::VarBit(read_optional_i32_type(cur)?),
        type_tag::JSON => ColumnType::Json,
        type_tag::XML => ColumnType::Xml,
        type_tag::JSONB => ColumnType::Jsonb,
        type_tag::JSONPATH => ColumnType::JsonPath,
        type_tag::ARRAY => ColumnType::Array(read_elem_type_with(cur, resolve_user_type)?),
        type_tag::USER => {
            let raw = take_n(cur, 4)?;
            let oid = u32::from_be_bytes(raw.try_into().expect("4 bytes fit u32"));
            if oid == crabka_pgtypes::oids::RECORD {
                ColumnType::Record(None)
            } else if oid == crabka_pgtypes::oids::NAME {
                ColumnType::Name
            } else if oid == crabka_pgtypes::oids::ACLITEM {
                ColumnType::Aclitem
            } else if oid == crabka_pgtypes::oids::REFCURSOR {
                ColumnType::Refcursor
            } else if let Some(builtin) = crabka_pgtypes::ColumnType::builtin_range(oid)
                .or_else(|| crabka_pgtypes::ColumnType::builtin_multirange(oid))
                .or_else(|| crabka_pgtypes::ColumnType::information_schema_domain_by_oid(oid))
            {
                builtin
            } else {
                resolve_user_type(oid).ok_or(UserTypeDecodeError::UnresolvedUserType(oid))?
            }
        }
        other => {
            return Err(KvError::CorruptRow(format!("unknown column type tag {other}")).into());
        }
    })
}

fn read_elem_type_with(
    cur: &mut &[u8],
    resolve_user_type: &dyn Fn(u32) -> Option<ColumnType>,
) -> Result<crabka_pgtypes::ElemType, UserTypeDecodeError> {
    let Some(code) = cur.first().copied() else {
        return Err(KvError::CorruptRow("unknown array element type encoding".into()).into());
    };
    if !matches!(code, 18 | 19 | 25 | 26) {
        return crabka_pgtypes::ElemType::read_code(cur).ok_or_else(|| {
            KvError::CorruptRow("unknown array element type encoding".into()).into()
        });
    }

    let _ = take_u8(cur)?;
    let oid = u32::from_be_bytes(
        take_n(cur, 4)?
            .try_into()
            .expect("4 bytes fit user array element oid"),
    );
    let ty = if code == 18 {
        ColumnType::builtin_range(oid).or_else(|| resolve_user_type(oid))
    } else if code == 19 {
        ColumnType::builtin_multirange(oid).or_else(|| resolve_user_type(oid))
    } else if code == 25 && oid == crabka_pgtypes::oids::RECORD {
        Some(ColumnType::Record(None))
    } else {
        resolve_user_type(oid)
    }
    .ok_or(UserTypeDecodeError::UnresolvedUserType(oid))?;
    match (code, ty) {
        (18, ColumnType::Range(range)) => Ok(crabka_pgtypes::ElemType::Range(range)),
        (19, ColumnType::Multirange(multirange)) => {
            Ok(crabka_pgtypes::ElemType::Multirange(multirange))
        }
        (25, ColumnType::Record(record)) => Ok(crabka_pgtypes::ElemType::Record(record)),
        (26, ColumnType::Enum(user)) => Ok(crabka_pgtypes::ElemType::User(user)),
        (26, ColumnType::Domain(domain)) => Ok(crabka_pgtypes::ElemType::User(domain.as_ref())),
        (26, ColumnType::Base(base)) => Ok(crabka_pgtypes::ElemType::User(base.as_ref())),
        _ => Err(
            KvError::CorruptRow(format!("array element oid {oid} has the wrong type kind")).into(),
        ),
    }
}

/// The same shape as [`write_optional_u16_type`] over the wider range a
/// `bit(n)` length modifier occupies.
fn write_optional_i32_type(out: &mut Vec<u8>, tag: u8, value: Option<i32>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn read_optional_i32_type(cur: &mut &[u8]) -> Result<Option<i32>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(i32::from_be_bytes(
            take_n(cur, 4)?.try_into().expect("4"),
        ))),
        flag => Err(KvError::CorruptRow(format!(
            "unknown bit typmod flag {flag}"
        ))),
    }
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
        ColumnDefault::Expression(source) => {
            out.push(3);
            write_str(out, source);
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
        Datum::JsonPath(value) => {
            out.push(datum_tag::JSONPATH);
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
        Datum::Json(text) => {
            out.push(datum_tag::JSON);
            write_str(out, text);
        }
        Datum::Xml(text) => {
            out.push(datum_tag::XML);
            write_str(out, text);
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
        Datum::PgSnapshot(value) => {
            out.push(datum_tag::PG_SNAPSHOT);
            write_str(out, &value.to_string());
        }
        Datum::TsQuery(value) => {
            out.push(datum_tag::TSQUERY);
            write_str(out, &value.to_string());
        }
        // The row encoder already distinguishes the four network types and
        // keeps `inet`'s `is_cidr` flag, so a default round-trips through it.
        Datum::Money(value) => {
            out.push(datum_tag::MONEY);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Datum::InternalChar(value) => {
            out.push(datum_tag::INTERNAL_CHAR);
            out.push(*value);
        }
        // The row encoder keeps the bit count and the `varying` flag, so a
        // `bit`/`bit varying` default round-trips through it.
        Datum::BitString(_) => {
            out.push(datum_tag::BITSTRING);
            write_bytes(
                out,
                &crabka_pgkv::rowenc::encode_row(std::slice::from_ref(default)),
            );
        }
        Datum::Inet(_) | Datum::MacAddr(_) | Datum::MacAddr8(_) => {
            out.push(datum_tag::NETWORK);
            write_bytes(
                out,
                &crabka_pgkv::rowenc::encode_row(std::slice::from_ref(default)),
            );
        }
        Datum::Oid(_)
        | Datum::Xid(_)
        | Datum::Xid8(_)
        | Datum::Cid(_)
        | Datum::Tid(_)
        | Datum::PgLsn(_) => {
            out.push(datum_tag::SYSID);
            write_bytes(
                out,
                &crabka_pgkv::rowenc::encode_row(std::slice::from_ref(default)),
            );
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
        | Datum::Polygon(_)
        | Datum::Lseg(_)
        | Datum::Line(_)
        | Datum::Circle(_)
        | Datum::Box(_)
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
        3 => Some(ColumnDefault::Expression(read_string(cur)?)),
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
        datum_tag::JSONPATH => Datum::JsonPath(read_string(cur)?),
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
        datum_tag::JSON => Datum::Json(read_string(cur)?),
        datum_tag::XML => Datum::Xml(read_string(cur)?),
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
        datum_tag::PG_SNAPSHOT => {
            Datum::PgSnapshot(Box::new(read_string(cur)?.parse().map_err(|error| {
                KvError::CorruptRow(format!("invalid pg_snapshot default: {error}"))
            })?))
        }
        datum_tag::TSQUERY => {
            Datum::TsQuery(read_string(cur)?.parse().map_err(|error| {
                KvError::CorruptRow(format!("invalid tsquery default: {error}"))
            })?)
        }
        datum_tag::MONEY => Datum::Money(i64::from_be_bytes(
            take_n(cur, 8)?
                .try_into()
                .map_err(|_| KvError::CorruptRow("invalid money default".into()))?,
        )),
        datum_tag::INTERNAL_CHAR => Datum::InternalChar(take_u8(cur)?),
        datum_tag::BITSTRING => {
            let mut values = crabka_pgkv::rowenc::decode_row(read_str(cur)?)?;
            if values.len() != 1 || !matches!(values.first(), Some(Datum::BitString(_))) {
                return Err(KvError::CorruptRow("invalid bit string default".into()));
            }
            values.pop().expect("length checked")
        }
        datum_tag::NETWORK => {
            let mut values = crabka_pgkv::rowenc::decode_row(read_str(cur)?)?;
            if values.len() != 1
                || !matches!(
                    values.first(),
                    Some(Datum::Inet(_) | Datum::MacAddr(_) | Datum::MacAddr8(_))
                )
            {
                return Err(KvError::CorruptRow(
                    "invalid network address default".into(),
                ));
            }
            values.pop().expect("length checked")
        }
        datum_tag::SYSID => {
            let mut values = crabka_pgkv::rowenc::decode_row(read_str(cur)?)?;
            if values.len() != 1
                || !matches!(
                    values.first(),
                    Some(
                        Datum::Oid(_)
                            | Datum::Xid(_)
                            | Datum::Xid8(_)
                            | Datum::Cid(_)
                            | Datum::Tid(_)
                            | Datum::PgLsn(_)
                    )
                )
            {
                return Err(KvError::CorruptRow(
                    "invalid system identifier default".into(),
                ));
            }
            values.pop().expect("length checked")
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

/// Write a relation name as its two length-prefixed halves.
///
/// A stored name is never a dotted string, for the same reason a catalog key is
/// not: nothing can recover the two halves from one string.
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

fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            write_str(out, value);
        }
        None => out.push(0),
    }
}

fn write_optional_type(out: &mut Vec<u8>, value: Option<ColumnType>) {
    match value {
        Some(value) => {
            out.push(1);
            write_type(out, value);
        }
        None => out.push(0),
    }
}

/// Append a length-prefixed byte string, the framing `read_str` expects.
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

fn read_optional_string(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => read_string(cur).map(Some),
        tag => Err(KvError::CorruptRow(format!(
            "unknown optional string tag {tag}"
        ))),
    }
}

fn read_optional_type(
    cur: &mut &[u8],
    resolve_user_type: &dyn Fn(u32) -> Option<ColumnType>,
) -> Result<Option<ColumnType>, UserTypeDecodeError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => read_type_with(cur, resolve_user_type).map(Some),
        tag => Err(UserTypeDecodeError::Corrupt(KvError::CorruptRow(format!(
            "unknown optional type tag {tag}"
        )))),
    }
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

/// Serialize a table schema (ordinary, foreign, or materialized view).
///
/// Always writes [`SCHEMA_VERSION`], then the column list, table option flags,
/// the owning role, and a foreign flag: `0` for an ordinary table, `1` for a
/// foreign table followed by the foreign metadata payload. The `CHECK`
/// constraint list follows, and then a materialized flag: `0` for a relation
/// that is not a materialized view, `1` for one followed by its definition and
/// `relispopulated` byte.
///
/// The two relation-kind payloads are written independently even though a
/// relation carries at most one of them — the encoder does not police the
/// exclusion, so a decoder never has to guess which flag won.
///
/// # Panics
///
/// Panics when a catalog collection or string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_schema(
    table_id: u32,
    columns: &[Column],
    options: TableOptions,
    owner: &str,
    meta: Option<&ForeignTableMeta>,
    materialized: Option<&MaterializedView>,
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
        write_generated(&mut out, c.generated.as_ref());
        out.push(identity_flag(c.identity));
        write_collation(&mut out, c.collation.as_deref());
        out.extend_from_slice(&c.statistics_target.to_be_bytes());
        match c.storage {
            None => out.push(0),
            Some(storage) => {
                out.push(1);
                out.push(storage);
            }
        }
        write_options(&mut out, &c.attribute_options);
    }
    out.push(table_option_flags(options));
    write_str(&mut out, owner);
    match meta {
        None => out.push(0),
        Some(m) => {
            out.push(1);
            write_str(&mut out, &m.server);
            write_options(&mut out, &m.options);
            out.extend_from_slice(
                &u32::try_from(m.column_options.len())
                    .expect("foreign column-option count must fit in u32")
                    .to_be_bytes(),
            );
            for (column, options) in &m.column_options {
                write_str(&mut out, column);
                write_options(&mut out, options);
            }
        }
    }
    write_checks(&mut out, checks);
    match materialized {
        None => out.push(0),
        Some(m) => {
            out.push(1);
            write_str(&mut out, &m.definition);
            out.push(u8::from(m.populated));
        }
    }
    out
}

const GENERATED_NONE: u8 = 0;
const GENERATED_STORED: u8 = 1;
const GENERATED_VIRTUAL: u8 = 2;

/// `GENERATED ALWAYS AS (expr)`: a kind byte — [`GENERATED_NONE`] for a column
/// that is not generated, [`GENERATED_STORED`] for `STORED`, and
/// [`GENERATED_VIRTUAL`] for `VIRTUAL` — followed by the expression source for
/// the two generated kinds.
fn write_generated(out: &mut Vec<u8>, generated: Option<&GeneratedColumn>) {
    match generated {
        None => out.push(GENERATED_NONE),
        Some(g) => {
            out.push(match g.kind {
                GeneratedKind::Stored => GENERATED_STORED,
                GeneratedKind::Virtual => GENERATED_VIRTUAL,
            });
            write_str(out, &g.expr);
        }
    }
}

/// Reads back what [`write_generated`] wrote, refusing any kind byte outside
/// the three it defines.
fn read_generated(cur: &mut &[u8]) -> Result<Option<GeneratedColumn>, KvError> {
    let kind = match take_u8(cur)? {
        GENERATED_NONE => return Ok(None),
        GENERATED_STORED => GeneratedKind::Stored,
        GENERATED_VIRTUAL => GeneratedKind::Virtual,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown generated-column flag {flag}"
            )));
        }
    };
    Ok(Some(GeneratedColumn {
        expr: read_string(cur)?,
        kind,
    }))
}

const COLLATION_DEFAULT: u8 = 0;
const COLLATION_NAMED: u8 = 1;

/// A column's written `COLLATE "name"`: a presence byte, followed by the name
/// when one was written. `None` is the type's own default collation, which is
/// what every column had before the clause was accepted.
fn write_collation(out: &mut Vec<u8>, collation: Option<&str>) {
    match collation {
        None => out.push(COLLATION_DEFAULT),
        Some(name) => {
            out.push(COLLATION_NAMED);
            write_str(out, name);
        }
    }
}

/// Reads back what [`write_collation`] wrote, refusing any presence byte
/// outside the two it writes.
fn read_collation(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    match take_u8(cur)? {
        COLLATION_DEFAULT => Ok(None),
        COLLATION_NAMED => Ok(Some(read_string(cur)?)),
        flag => Err(KvError::CorruptRow(format!(
            "unknown column-collation flag {flag}"
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
/// predicate source, `pg_constraint.convalidated`, and `connoinherit` flags.
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
        out.push(u8::from(check.no_inherit));
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
        let no_inherit = match take_u8(cur)? {
            0 => false,
            1 => true,
            flag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown check-constraint no-inherit flag {flag}"
                )));
            }
        };
        checks.push(CheckConstraint {
            name,
            expr,
            validated,
            no_inherit,
        });
    }
    Ok(checks)
}

fn table_option_flags(options: TableOptions) -> u8 {
    let mut flags = 0;
    if options.sharded {
        flags |= TABLE_OPTION_SHARDED;
    }
    if options.row_security {
        flags |= TABLE_OPTION_ROW_SECURITY;
    }
    if options.force_row_security {
        flags |= TABLE_OPTION_FORCE_ROW_SECURITY;
    }
    flags
}

/// Decode the table-option byte, refusing any bit this build does not know.
///
/// The strictness is a security property, not tidiness. Row security lives in
/// this byte, and a tolerant reader — one that masked unknown bits away — would
/// decode a record written with a later flag layout as a table with row
/// security *off*, which is a silent, total policy bypass. Refusing the record
/// fails the read instead.
fn read_table_options(flags: u8) -> Result<TableOptions, KvError> {
    if flags & !TABLE_OPTION_KNOWN != 0 {
        return Err(KvError::CorruptRow(format!(
            "unknown table option flags {flags:#04x}"
        )));
    }
    Ok(TableOptions {
        sharded: flags & TABLE_OPTION_SHARDED != 0,
        row_security: flags & TABLE_OPTION_ROW_SECURITY != 0,
        force_row_security: flags & TABLE_OPTION_FORCE_ROW_SECURITY != 0,
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
    out.push(u8::from(index.without_overlaps));
    out.push(u8::from(index.clustered));
    out.push(match index.deferral {
        ConstraintDeferral::Immediate => INDEX_DEFERRAL_IMMEDIATE,
        ConstraintDeferral::Deferrable => INDEX_DEFERRAL_DEFERRABLE,
        ConstraintDeferral::Deferred => INDEX_DEFERRAL_DEFERRED,
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
    match &index.predicate {
        Some(predicate) => {
            out.push(1);
            write_str(&mut out, predicate);
        }
        None => out.push(0),
    }
    out.extend_from_slice(
        &u32::try_from(index.include.len())
            .expect("index include count must fit in u32")
            .to_be_bytes(),
    );
    for column in &index.include {
        write_str(&mut out, column);
    }
    out.push(u8::from(index.nulls_not_distinct));
    out.extend_from_slice(
        &u32::try_from(index.key_options.len())
            .expect("index key-option count must fit in u32")
            .to_be_bytes(),
    );
    for option in &index.key_options {
        out.push(u8::from(option.descending));
        out.push(u8::from(option.nulls_first));
        for name in [&option.opclass, &option.collation] {
            match name {
                Some(name) => {
                    out.push(1);
                    write_str(&mut out, name);
                }
                None => out.push(0),
            }
        }
        match &option.opclass_options {
            Some(options) => {
                out.push(1);
                write_str(&mut out, options);
            }
            None => out.push(0),
        }
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
    if version != INDEX_VERSION
        && version != INDEX_VERSION_KEY_OPTIONS
        && version != INDEX_VERSION_INCLUDE
        && version != INDEX_VERSION_PREDICATE
        && version != INDEX_VERSION_LEGACY
    {
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
    let method = match take_u8(&mut cur)? {
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
    };
    let without_overlaps = match take_u8(&mut cur)? {
        0 => false,
        1 => true,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown index WITHOUT OVERLAPS flag {flag}"
            )));
        }
    };
    let clustered = match take_u8(&mut cur)? {
        0 => false,
        1 => true,
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown index clustered flag {flag}"
            )));
        }
    };
    let deferral = match take_u8(&mut cur)? {
        INDEX_DEFERRAL_IMMEDIATE => ConstraintDeferral::Immediate,
        INDEX_DEFERRAL_DEFERRABLE => ConstraintDeferral::Deferrable,
        INDEX_DEFERRAL_DEFERRED => ConstraintDeferral::Deferred,
        tag => {
            return Err(KvError::CorruptRow(format!(
                "unknown index deferral tag {tag}"
            )));
        }
    };
    let constraint = match take_u8(&mut cur)? {
        INDEX_CONSTRAINT_NONE => None,
        INDEX_CONSTRAINT_PRIMARY_KEY => Some(IndexConstraint::PrimaryKey),
        INDEX_CONSTRAINT_UNIQUE => Some(IndexConstraint::Unique),
        INDEX_CONSTRAINT_EXCLUSION => {
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
    let predicate = if version == INDEX_VERSION_LEGACY {
        None
    } else {
        match take_u8(&mut cur)? {
            0 => None,
            1 => Some(read_string(&mut cur)?),
            tag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown index predicate flag {tag}"
                )));
            }
        }
    };
    let include = if version >= INDEX_VERSION_INCLUDE {
        let count = usize::try_from(u32::from_be_bytes(
            take_n(&mut cur, 4)?.try_into().expect("4"),
        ))
        .expect("u32 fits in usize on supported targets");
        let mut include = Vec::with_capacity(count.min(16));
        for _ in 0..count {
            include.push(read_string(&mut cur)?);
        }
        include
    } else {
        Vec::new()
    };
    let nulls_not_distinct = if version >= INDEX_VERSION_KEY_OPTIONS {
        match take_u8(&mut cur)? {
            0 => false,
            1 => true,
            flag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown index NULLS NOT DISTINCT flag {flag}"
                )));
            }
        }
    } else {
        false
    };
    let key_options = if version >= INDEX_VERSION_KEY_OPTIONS {
        let count = usize::try_from(u32::from_be_bytes(
            take_n(&mut cur, 4)?.try_into().expect("4"),
        ))
        .expect("u32 fits in usize on supported targets");
        if count != columns.len() {
            return Err(KvError::CorruptRow(
                "index key-option count does not match index columns".into(),
            ));
        }
        let mut options = Vec::with_capacity(count.min(16));
        for _ in 0..count {
            let descending = read_index_flag(&mut cur, "descending")?;
            let nulls_first = read_index_flag(&mut cur, "NULLS FIRST")?;
            let read_name = |cur: &mut &[u8]| match take_u8(cur)? {
                0 => Ok(None),
                1 => Ok(Some(read_string(cur)?)),
                flag => Err(KvError::CorruptRow(format!(
                    "unknown index key-option name flag {flag}"
                ))),
            };
            options.push(crate::IndexKeyOptions {
                descending,
                nulls_first,
                opclass: read_name(&mut cur)?,
                collation: read_name(&mut cur)?,
                opclass_options: if version >= INDEX_VERSION {
                    read_name(&mut cur)?
                } else {
                    None
                },
            });
        }
        options
    } else {
        crate::default_index_key_options(columns.len())
    };
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
        key_options,
        include,
        predicate,
        nulls_not_distinct,
        unique,
        placement,
        method,
        constraint,
        without_overlaps,
        clustered,
        deferral,
    })
}

fn read_index_flag(cur: &mut &[u8], what: &str) -> Result<bool, KvError> {
    match take_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(KvError::CorruptRow(format!(
            "unknown index {what} flag {flag}"
        ))),
    }
}

// ── Foreign keys ──────────────────────────────────────────────────────────────

/// Serialize a foreign-key constraint record.
///
/// The record starts with a version byte. Then come the constraint's own
/// creation-order id and its name, the child relation's display name and its
/// id, the referenced relation's display name and its id, and the referenced
/// unique index's display name and its id.
///
/// The match type and the `ON DELETE` and `ON UPDATE` actions follow as one
/// byte each. The deferrable, initially-deferred and validated flags follow as
/// one byte each. The referencing, referenced and `SET NULL`/`SET DEFAULT`
/// column-name lists close the record. Each list is a `u32` count followed by
/// that many length-prefixed names.
///
/// The ids are the authority, and the display names are denormalized copies
/// that a rename rewrites. The record is therefore self-describing without a
/// second lookup.
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

/// Deserialize a table schema into a [`DecodedSchema`].
///
/// Returns `KvError::CorruptRow` if the version byte is not [`SCHEMA_VERSION`],
/// if the table option flags contain unknown bits, if the foreign flag after
/// the owner is not `0` (ordinary) or `1` (foreign), or if the materialized
/// flag closing the record is not `0` (not a materialized view) or `1` (a
/// materialized view, followed by its definition and populated byte).
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
        let collation = read_collation(&mut cur)?;
        let statistics_target = i16::from_be_bytes(take_n(&mut cur, 2)?.try_into().expect("2"));
        let storage = match take_u8(&mut cur)? {
            0 => None,
            1 => Some(take_u8(&mut cur)?),
            flag => {
                return Err(KvError::CorruptRow(format!(
                    "unknown column-storage flag {flag}"
                )));
            }
        };
        let attribute_options = read_options(&mut cur)?;
        columns.push(Column {
            name,
            ty,
            not_null,
            default,
            generated,
            identity,
            collation,
            statistics_target,
            storage,
            attribute_options,
        });
    }
    let options = read_table_options(take_u8(&mut cur)?)?;
    let owner = read_string(&mut cur)?;
    let foreign = match take_u8(&mut cur)? {
        0 => None,
        1 => {
            let server = read_string(&mut cur)?;
            let options = read_options(&mut cur)?;
            let count = usize::try_from(u32::from_be_bytes(
                take_n(&mut cur, 4)?.try_into().expect("4"),
            ))
            .expect("u32 fits in usize on supported targets");
            let mut column_options = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                column_options.push((read_string(&mut cur)?, read_options(&mut cur)?));
            }
            Some(ForeignTableMeta {
                server,
                options,
                column_options,
            })
        }
        flag => {
            return Err(KvError::CorruptRow(format!("unknown foreign flag {flag}")));
        }
    };
    let checks = read_checks(&mut cur)?;
    let materialized = match take_u8(&mut cur)? {
        0 => None,
        1 => {
            let definition = read_string(&mut cur)?;
            let populated = match take_u8(&mut cur)? {
                0 => false,
                1 => true,
                flag => {
                    return Err(KvError::CorruptRow(format!(
                        "unknown materialized populated flag {flag}"
                    )));
                }
            };
            Some(MaterializedView {
                definition,
                populated,
            })
        }
        flag => {
            return Err(KvError::CorruptRow(format!(
                "unknown materialized flag {flag}"
            )));
        }
    };
    Ok((
        table_id,
        columns,
        options,
        owner,
        foreign,
        checks,
        materialized,
    ))
}

// ── Foreign-data wrapper ──────────────────────────────────────────────────────

/// Format: `oid | name | owner | handler | validator | options`.
#[must_use]
pub fn serialize_fdw(
    oid: u32,
    name: &str,
    owner: &str,
    handler: Option<&str>,
    validator: Option<&str>,
    options: &[(String, String)],
) -> Vec<u8> {
    let mut out = oid.to_be_bytes().to_vec();
    write_str(&mut out, name);
    write_str(&mut out, owner);
    write_optional_string(&mut out, handler);
    write_optional_string(&mut out, validator);
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
    let oid = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let name = read_string(&mut cur)?;
    let owner = read_string(&mut cur)?;
    let handler = read_optional_string(&mut cur)?;
    let validator = read_optional_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(ForeignDataWrapper {
        oid,
        name,
        owner,
        handler,
        validator,
        options,
    })
}

// ── User-defined types ────────────────────────────────────────────────────────

/// Serialize a user-defined type: `oid`, its legacy flattened lookup name, a
/// kind byte, the kind's payload, then a versioned structured identity trailer.
/// Keeping the legacy name first lets an older binary read a new record, while
/// the trailer preserves identifiers containing dots without reconstruction.
#[must_use]
pub fn serialize_user_type(ty: &UserType) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&ty.oid.to_be_bytes());
    write_str(&mut out, &ty.qualified_name());
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
        UserTypeBody::EnumOrdered {
            labels,
            sort_orders,
        } => {
            out.push(USER_TYPE_ENUM_ORDERED);
            write_count(&mut out, labels.len());
            for (label, sort_order) in labels.iter().zip(sort_orders) {
                write_str(&mut out, label);
                out.extend_from_slice(&sort_order.to_be_bytes());
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
            match (&range.multirange_schema, &range.multirange_name) {
                (Some(schema), Some(name)) => {
                    out.push(1);
                    write_str(
                        &mut out,
                        &if schema == crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA {
                            name.clone()
                        } else {
                            format!("{schema}.{name}")
                        },
                    );
                }
                _ => out.push(0),
            }
        }
        UserTypeBody::Domain(domain) => {
            out.push(USER_TYPE_DOMAIN_V3);
            write_type(&mut out, domain.base);
            out.push(u8::from(domain.not_null));
            match &domain.not_null_name {
                Some(name) => {
                    out.push(1);
                    write_str(&mut out, name);
                }
                None => out.push(0),
            }
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
                out.push(u8::from(check.validated));
            }
        }
        UserTypeBody::Shell => out.push(USER_TYPE_SHELL),
        UserTypeBody::Base(base) => {
            out.push(USER_TYPE_BASE);
            write_type(&mut out, base.representation);
            out.extend_from_slice(&base.layout.length.to_be_bytes());
            out.push(u8::from(base.layout.by_value));
            out.push(base.layout.alignment as u8);
            write_optional_type(&mut out, base.element);
            write_optional_string(&mut out, base.default.as_deref());
            write_str(&mut out, &base.input);
            write_str(&mut out, &base.output);
            write_optional_string(&mut out, base.typmod_in.as_deref());
            write_optional_string(&mut out, base.typmod_out.as_deref());
            write_str(&mut out, &base.category);
            out.push(u8::from(base.preferred));
            write_str(&mut out, &base.delimiter);
            out.push(base.storage as u8);
        }
    }
    out.push(USER_TYPE_IDENTITY_V2);
    write_str(&mut out, &ty.schema);
    write_str(&mut out, &ty.name);
    match &ty.body {
        UserTypeBody::Range(RangeBody {
            multirange_schema: Some(schema),
            multirange_name: Some(name),
            ..
        }) => {
            out.push(1);
            write_str(&mut out, schema);
            write_str(&mut out, name);
        }
        _ => out.push(0),
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
/// This function panics if a fixed-width field's slice is not the width the
/// reader just asked for. That cannot happen: `take_n` either yields exactly
/// that many bytes or returns the corruption error above.
pub fn deserialize_user_type(bytes: &[u8]) -> Result<UserType, KvError> {
    deserialize_user_type_with(bytes, &crabka_pgtypes::usertype::column_type_for_oid)
        .map_err(UserTypeDecodeError::into_kv_error)
}

pub(crate) fn deserialize_user_type_with(
    bytes: &[u8],
    resolve_user_type: &dyn Fn(u32) -> Option<ColumnType>,
) -> Result<UserType, UserTypeDecodeError> {
    let mut cur = bytes;
    let oid = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4 bytes fit u32"));
    let legacy_name = read_string(&mut cur)?;
    let kind = take_u8(&mut cur)?;
    let mut body = match kind {
        USER_TYPE_COMPOSITE => {
            let count = read_count(&mut cur)?;
            let mut fields = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let field_name = read_string(&mut cur)?;
                fields.push(CompositeField {
                    name: field_name,
                    ty: read_type_with(&mut cur, resolve_user_type)?,
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
        USER_TYPE_ENUM_ORDERED => {
            let count = read_count(&mut cur)?;
            let mut labels = Vec::with_capacity(count.min(1024));
            let mut sort_orders = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                labels.push(read_string(&mut cur)?);
                sort_orders.push(u32::from_be_bytes(
                    take_n(&mut cur, 4)?.try_into().expect("4 bytes fit u32"),
                ));
            }
            UserTypeBody::EnumOrdered {
                labels,
                sort_orders,
            }
        }
        USER_TYPE_RANGE => {
            let subtype = read_type_with(&mut cur, resolve_user_type)?;
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
                multirange_schema: None,
                multirange_name,
            })
        }
        USER_TYPE_DOMAIN | USER_TYPE_DOMAIN_V2 | USER_TYPE_DOMAIN_V3 => {
            let base = read_type_with(&mut cur, resolve_user_type)?;
            let not_null = take_u8(&mut cur)? != 0;
            let not_null_name = if kind == USER_TYPE_DOMAIN_V3 {
                match take_u8(&mut cur)? {
                    0 => None,
                    1 => Some(read_string(&mut cur)?),
                    value => {
                        return Err(KvError::CorruptRow(format!(
                            "unknown domain NOT NULL name tag {value}"
                        ))
                        .into());
                    }
                }
            } else {
                None
            };
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
                    validated: kind == USER_TYPE_DOMAIN || take_u8(&mut cur)? != 0,
                });
            }
            UserTypeBody::Domain(DomainBody {
                base,
                not_null,
                not_null_name,
                default,
                checks,
            })
        }
        USER_TYPE_SHELL => UserTypeBody::Shell,
        USER_TYPE_BASE => {
            let representation = read_type_with(&mut cur, resolve_user_type)?;
            let layout = crabka_pgtypes::usertype::BaseLayout {
                length: i16::from_be_bytes(take_n(&mut cur, 2)?.try_into().expect("2")),
                by_value: take_u8(&mut cur)? != 0,
                alignment: char::from(take_u8(&mut cur)?),
            };
            let element = read_optional_type(&mut cur, resolve_user_type)?;
            let default = read_optional_string(&mut cur)?;
            let input = read_string(&mut cur)?;
            let output = read_string(&mut cur)?;
            let typmod_in = read_optional_string(&mut cur)?;
            let typmod_out = read_optional_string(&mut cur)?;
            let category = read_string(&mut cur)?;
            let preferred = take_u8(&mut cur)? != 0;
            let delimiter = read_string(&mut cur)?;
            let storage = char::from(take_u8(&mut cur)?);
            UserTypeBody::Base(BaseBody {
                representation,
                layout,
                element,
                default,
                input,
                output,
                typmod_in,
                typmod_out,
                category,
                preferred,
                delimiter,
                storage,
            })
        }
        other => {
            return Err(KvError::CorruptRow(format!("unknown user type kind {other}")).into());
        }
    };
    let (schema, name, structured_companion) = if cur.is_empty() {
        let (schema, name) = legacy_user_type_identity(&legacy_name);
        (schema, name, None)
    } else {
        let version = take_u8(&mut cur)?;
        match version {
            USER_TYPE_IDENTITY_V1 | USER_TYPE_IDENTITY_V2 => {}
            version => {
                return Err(KvError::CorruptRow(format!(
                    "unknown user type identity version {version}"
                ))
                .into());
            }
        }
        let schema = read_string(&mut cur)?;
        let name = read_string(&mut cur)?;
        let companion = if version == USER_TYPE_IDENTITY_V2 {
            match take_u8(&mut cur)? {
                0 => None,
                1 => Some((read_string(&mut cur)?, read_string(&mut cur)?)),
                flag => {
                    return Err(KvError::CorruptRow(format!(
                        "unknown user type companion flag {flag}"
                    ))
                    .into());
                }
            }
        } else {
            None
        };
        if !cur.is_empty() {
            return Err(
                KvError::CorruptRow("trailing bytes after user type identity".into()).into(),
            );
        }
        (schema, name, companion)
    };
    if let UserTypeBody::Range(range) = &mut body {
        if let Some((schema, name)) = structured_companion {
            range.multirange_schema = Some(schema);
            range.multirange_name = Some(name);
        } else if let Some(legacy_companion) = range.multirange_name.take() {
            let (schema, name) = legacy_user_type_identity(&legacy_companion);
            range.multirange_schema = Some(schema);
            range.multirange_name = Some(name);
        } else {
            // Records written before CREATE materialized PostgreSQL's derived
            // companion identity used the old suffix-replacement algorithm at
            // lookup time. Freeze that historical identity during migration so
            // upgrading does not silently rename an existing companion.
            range.multirange_schema = Some(schema.clone());
            range.multirange_name = Some(legacy_default_multirange_name(&name));
        }
    }
    Ok(UserType {
        oid,
        array_oid: crabka_pgtypes::usertype::user_array_oid(oid),
        schema,
        name,
        body,
    })
}

/// Decode records written before structured type identities were appended.
/// Such records could not distinguish a dotted identifier from qualification.
/// The permanent migration policy is the historical last-dot split: it keeps
/// every unambiguous old identity stable, while inherently ambiguous quoted-dot
/// records must be recreated or renamed by an operator. New records never rely
/// on this lossy representation.
fn legacy_user_type_identity(name: &str) -> (String, String) {
    name.rsplit_once('.').map_or_else(
        || {
            (
                crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA.to_string(),
                name.to_string(),
            )
        },
        |(schema, name)| (schema.to_string(), name.to_string()),
    )
}

fn legacy_default_multirange_name(range_name: &str) -> String {
    range_name.strip_suffix("range").map_or_else(
        || format!("{range_name}_multirange"),
        |stem| format!("{stem}multirange"),
    )
}

const USER_TYPE_COMPOSITE: u8 = 1;
const USER_TYPE_ENUM: u8 = 2;
const USER_TYPE_ENUM_ORDERED: u8 = 9;
const USER_TYPE_DOMAIN: u8 = 3;
/// Domain records with a per-constraint validation state. The old tag remains
/// readable and treats its checks as validated.
const USER_TYPE_DOMAIN_V2: u8 = 7;
/// Domain records with a named `NOT NULL` constraint.
const USER_TYPE_DOMAIN_V3: u8 = 8;
const USER_TYPE_RANGE: u8 = 4;
/// `CREATE TYPE name;` — a shell, with no body at all.
const USER_TYPE_SHELL: u8 = 5;
/// `CREATE TYPE name (INPUT = …, OUTPUT = …)` — a user-defined base type.
const USER_TYPE_BASE: u8 = 6;
const USER_TYPE_IDENTITY_V1: u8 = 1;
const USER_TYPE_IDENTITY_V2: u8 = 2;

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

/// Format: `oid | name | owner | wrapper | type | version | options`.
#[must_use]
pub fn serialize_server(
    oid: u32,
    name: &str,
    owner: &str,
    wrapper: &str,
    server_type: Option<&str>,
    version: Option<&str>,
    options: &[(String, String)],
) -> Vec<u8> {
    let mut out = oid.to_be_bytes().to_vec();
    write_str(&mut out, name);
    write_str(&mut out, owner);
    write_str(&mut out, wrapper);
    write_optional_string(&mut out, server_type);
    write_optional_string(&mut out, version);
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
    let oid = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let name = read_string(&mut cur)?;
    let owner = read_string(&mut cur)?;
    let wrapper = read_string(&mut cur)?;
    let server_type = read_optional_string(&mut cur)?;
    let version = read_optional_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(ForeignServer {
        oid,
        name,
        owner,
        wrapper,
        server_type,
        version,
        options,
    })
}

// ── User mapping ──────────────────────────────────────────────────────────────

/// Format: `oid | user len | user | server len | server | options`.
#[must_use]
pub fn serialize_user_mapping(
    oid: u32,
    user: &str,
    server: &str,
    options: &[(String, String)],
) -> Vec<u8> {
    let mut out = oid.to_be_bytes().to_vec();
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
    let oid = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let user = read_string(&mut cur)?;
    let server = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(UserMapping {
        oid,
        user,
        server,
        options,
    })
}

// ── Views ─────────────────────────────────────────────────────────────────────

const VIEW_VERSION: u8 = 4;

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
    write_str(&mut out, &view.owner);
    out.extend_from_slice(
        &u32::try_from(view.columns.len())
            .expect("view column count must fit in u32")
            .to_be_bytes(),
    );
    for column in &view.columns {
        write_str(&mut out, &column.name);
        write_type(&mut out, column.ty);
    }
    out.push(u8::from(view.options.security_invoker));
    out.push(u8::from(view.options.security_barrier));
    out.push(match view.options.check_option {
        None => 0,
        Some(ViewCheckOption::Local) => 1,
        Some(ViewCheckOption::Cascaded) => 2,
    });
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
    let owner = read_string(&mut cur)?;
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
            // A view's stored column list is name and type only: the collation a
            // view column derives from its body is not persisted, so `\d` on a
            // view reports the type's own.
            collation: None,
            statistics_target: -1,
            storage: None,
            attribute_options: Vec::new(),
        });
    }
    let options = ViewOptions {
        security_invoker: take_u8(&mut cur)? != 0,
        security_barrier: take_u8(&mut cur)? != 0,
        check_option: match take_u8(&mut cur)? {
            0 => None,
            1 => Some(ViewCheckOption::Local),
            2 => Some(ViewCheckOption::Cascaded),
            other => {
                return Err(KvError::CorruptRow(format!(
                    "unknown view check option {other}"
                )));
            }
        },
    };
    if !cur.is_empty() {
        return Err(KvError::CorruptRow("trailing bytes in view record".into()));
    }
    Ok(View {
        name,
        definition,
        owner,
        columns,
        options,
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
    use crate::{
        Column, ForeignTableMeta, IndexKeyOptions, RelationName, default_index_key_options,
    };

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
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            },
            Column {
                name: "ratio".into(),
                ty: ColumnType::Numeric(None),
                not_null: false,
                default: None,
                generated: None,
                identity: None,
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            },
            Column::new("code", ColumnType::Varchar(Some(8))),
            Column::new("flag", ColumnType::Char(Some(2))),
            Column::new("public_id", ColumnType::Uuid),
            // A written `COLLATE` survives the round trip, and so does the
            // absence of one on a column of the very same type — the two are
            // distinct states, not one nullable string that defaults.
            Column {
                name: "sorted".into(),
                ty: ColumnType::Text,
                not_null: false,
                default: None,
                generated: None,
                identity: None,
                collation: Some("POSIX".into()),
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            },
        ];
        let bytes = serialize_schema(
            table_id,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (id, cols, options, _owner, foreign, ..) = deserialize_schema(&bytes).expect("decode");
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
            collation: None,
            statistics_target: -1,
            storage: None,
            attribute_options: Vec::new(),
        }];

        let bytes = serialize_schema(
            table_id,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (_id, decoded, _options, _owner, _foreign, ..) =
            deserialize_schema(&bytes).expect("decode");

        assert_eq!(decoded, columns);
    }

    /// A generated column's kind travels with its expression: `STORED` and
    /// `VIRTUAL` each come back as written, as does a column that is not
    /// generated at all.
    #[test]
    fn roundtrip_generated_column_kinds() {
        use assert2::assert;

        for generated in [
            None,
            Some(GeneratedColumn {
                expr: "id * 2".into(),
                kind: GeneratedKind::Stored,
            }),
            Some(GeneratedColumn {
                expr: "id + 1".into(),
                kind: GeneratedKind::Virtual,
            }),
        ] {
            let columns = vec![Column {
                name: "derived".into(),
                ty: ColumnType::Int4,
                not_null: false,
                default: None,
                generated,
                identity: None,
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            }];

            let bytes = serialize_schema(
                7,
                &columns,
                TableOptions::default(),
                "postgres",
                None,
                None,
                &[],
            );
            let (_id, decoded, _options, _owner, _foreign, ..) =
                deserialize_schema(&bytes).expect("decode");

            assert!(decoded == columns);
        }
    }

    /// A generated-column kind byte outside the three the encoder writes comes
    /// from a build this one does not understand. The reader must refuse it
    /// rather than guess a kind — reading a `VIRTUAL` column as stored would
    /// hand back the NULL placeholder as the column's value.
    #[test]
    fn unknown_generated_flag_byte_errors() {
        use assert2::assert;

        let encode = |generated| {
            serialize_schema(
                1,
                &[Column {
                    name: "x".into(),
                    ty: ColumnType::Int4,
                    not_null: false,
                    default: None,
                    generated,
                    identity: None,
                    collation: None,
                    statistics_target: -1,
                    storage: None,
                    attribute_options: Vec::new(),
                }],
                TableOptions::default(),
                "postgres",
                None,
                None,
                &[],
            )
        };
        let mut bytes = encode(None);
        // A generated column changes exactly one byte before it appends its
        // expression, which locates the kind byte without restating the layout.
        let kind_offset = bytes
            .iter()
            .zip(encode(Some(GeneratedColumn {
                expr: String::new(),
                kind: GeneratedKind::Stored,
            })))
            .position(|(absent, stored)| *absent != stored)
            .expect("a generated column changes the record");
        bytes[kind_offset] = 3;

        assert!(deserialize_schema(&bytes).is_err());
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
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
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
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
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
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
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
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            },
            Column {
                name: "path".into(),
                ty: ColumnType::JsonPath,
                not_null: false,
                default: Some(ColumnDefault::Value(Datum::JsonPath("$.\"a\"".into()))),
                generated: None,
                identity: None,
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            },
            Column {
                name: "paths".into(),
                ty: ColumnType::Array(ElemType::JsonPath),
                not_null: false,
                default: Some(ColumnDefault::Value(Datum::Array(ArrayValue::new(
                    ElemType::JsonPath,
                    vec![Datum::JsonPath("$.\"a\"".into()), Datum::Null],
                )))),
                generated: None,
                identity: None,
                collation: None,
                statistics_target: -1,
                storage: None,
                attribute_options: Vec::new(),
            },
        ];

        let bytes = serialize_schema(
            31,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (_id, decoded, _options, _owner, _foreign, ..) =
            deserialize_schema(&bytes).expect("decode");

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
        let bytes = serialize_schema(
            table_id,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (id, cols, options, _owner, foreign, ..) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(!options.sharded);
        assert!(foreign.is_none());
    }

    /// Every `jsonb`/array column type survives a catalog round trip. The
    /// element code is what separates one array column from another, so this
    /// test exercises all of them, not just one.
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
        columns.push(Column::new(
            "records",
            ColumnType::Array(ElemType::Record(None)),
        ));
        let ColumnType::Range(range) =
            ColumnType::builtin_range(crabka_pgtypes::oids::INT8RANGE).expect("int8range")
        else {
            unreachable!()
        };
        columns.push(Column::new(
            "ranges",
            ColumnType::Array(ElemType::Range(range)),
        ));
        let bytes = serialize_schema(
            table_id,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (id, cols, _options, _owner, _foreign, ..) =
            deserialize_schema(&bytes).expect("decode");
        assert!(id == table_id);
        assert!(cols == columns);
    }

    #[test]
    fn roundtrip_schema_with_every_user_array_element_kind() {
        let users = [
            crabka_pgtypes::usertype::UserType {
                oid: 301_120,
                array_oid: crabka_pgtypes::usertype::user_array_oid(301_120),
                schema: crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA.into(),
                name: "serde_enum_array".into(),
                body: crabka_pgtypes::usertype::UserTypeBody::Enum(vec!["ok".into()]),
            },
            crabka_pgtypes::usertype::UserType {
                oid: 301_124,
                array_oid: crabka_pgtypes::usertype::user_array_oid(301_124),
                schema: crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA.into(),
                name: "serde_domain_array".into(),
                body: crabka_pgtypes::usertype::UserTypeBody::Domain(
                    crabka_pgtypes::usertype::DomainBody {
                        base: ColumnType::Int4,
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        checks: Vec::new(),
                    },
                ),
            },
            crabka_pgtypes::usertype::UserType {
                oid: 301_128,
                array_oid: crabka_pgtypes::usertype::user_array_oid(301_128),
                schema: crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA.into(),
                name: "serde_base_array".into(),
                body: crabka_pgtypes::usertype::UserTypeBody::Base(
                    crabka_pgtypes::usertype::BaseBody {
                        representation: ColumnType::Int4,
                        layout: crabka_pgtypes::usertype::BaseLayout::from_representation(
                            ColumnType::Int4,
                        ),
                        element: None,
                        default: None,
                        input: "int4in".into(),
                        output: "int4out".into(),
                        typmod_in: None,
                        typmod_out: None,
                        category: "N".into(),
                        preferred: false,
                        delimiter: ",".into(),
                        storage: 'x',
                    },
                ),
            },
        ];
        for user in &users {
            crabka_pgtypes::usertype::replace(user);
        }
        let columns = users
            .iter()
            .map(|user| {
                Column::new(
                    &user.name,
                    ColumnType::Array(crabka_pgtypes::ElemType::User(user.type_ref())),
                )
            })
            .collect::<Vec<_>>();
        let bytes = serialize_schema(
            22,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );

        let (_, decoded, ..) = deserialize_schema(&bytes).expect("decode user array schema");
        assert_eq!(decoded, columns);

        let enum_type = users[0].type_ref();
        let cursor = [25]
            .into_iter()
            .chain(enum_type.oid.to_be_bytes())
            .collect::<Vec<_>>();
        assert!(
            read_elem_type_with(&mut cursor.as_slice(), &|oid| {
                (oid == enum_type.oid).then_some(ColumnType::Enum(enum_type))
            })
            .is_err()
        );
        for user in &users {
            crabka_pgtypes::usertype::unregister(&user.name);
        }
    }

    #[test]
    fn builtin_array_descriptors_roundtrip_through_schema_encoding() {
        let columns = [
            Column::new(
                "point_values",
                ColumnType::array_of(ColumnType::Point).expect("point[]"),
            ),
            Column::new(
                "bit_values",
                ColumnType::array_of(ColumnType::Bit(Some(3))).expect("bit(3)[]"),
            ),
            Column::new(
                "role_values",
                ColumnType::array_of(ColumnType::Regrole).expect("regrole[]"),
            ),
            Column::new(
                "multirange_values",
                ColumnType::array_of(
                    ColumnType::builtin_multirange(crabka_pgtypes::oids::INT4MULTIRANGE)
                        .expect("int4multirange"),
                )
                .expect("int4multirange[]"),
            ),
        ];
        let bytes = serialize_schema(
            23,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (_, decoded, ..) = deserialize_schema(&bytes).expect("decode builtin array schema");
        assert_eq!(decoded, columns);

        let mut malformed = vec![27];
        malformed.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(read_elem_type_with(&mut malformed.as_slice(), &|_| None).is_err());
    }

    #[test]
    fn unknown_array_element_code_is_a_corrupt_row() {
        use assert2::assert;

        let columns = vec![Column::new(
            "arr",
            ColumnType::Array(crabka_pgtypes::ElemType::Int4),
        )];
        let mut bytes = serialize_schema(
            3,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        // The element code is the byte after the ARRAY tag; corrupt it.
        let tag_at = bytes
            .iter()
            .enumerate()
            .skip(1)
            .find_map(|(index, byte)| (*byte == type_tag::ARRAY).then_some(index))
            .expect("array tag");
        bytes[tag_at + 1] = 200;
        assert!(deserialize_schema(&bytes).is_err());
    }

    /// Every `ColumnType` must survive `write_type`/`read_type` unchanged.
    ///
    /// The tag table is hand-maintained. Without this test, a new type whose
    /// tag collides with an existing one, or whose read arm is missing, would
    /// surface as a column that silently decodes to the wrong type rather than
    /// as a failure. This encoding was once reconstructed from its callers
    /// after an accidental revert. That is exactly the situation this test
    /// exists to catch.
    #[test]
    fn every_column_type_round_trips_through_its_tag() {
        let types = [
            ColumnType::Bool,
            ColumnType::Int2,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Text,
            ColumnType::Name,
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
            ColumnType::JsonPath,
            // All seven geometric types. `polygon` and `path` are the pair worth
            // watching: they hold the same shape of value, so a missing read arm
            // would let one decode as the other rather than as a failure.
            ColumnType::Point,
            ColumnType::Lseg,
            ColumnType::Path,
            ColumnType::Box,
            ColumnType::Polygon,
            ColumnType::Line,
            ColumnType::Circle,
            ColumnType::Record(None),
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
    ///
    /// The tag space is **append-only**: it is persisted, so a new type must
    /// take a byte no existing type uses and no existing byte may change
    /// meaning. Listing every distinctly-tagged type here is what turns a reused
    /// byte into a test failure instead of silently mis-decoded catalog rows.
    #[test]
    fn column_type_tags_are_distinct() {
        let types = [
            ColumnType::Bool,
            ColumnType::Int2,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Text,
            ColumnType::Name,
            ColumnType::Varchar(None),
            ColumnType::Char(None),
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
            ColumnType::Regtype,
            ColumnType::Regprocedure,
            ColumnType::Regnamespace,
            ColumnType::Regproc,
            ColumnType::Regoper,
            ColumnType::Regoperator,
            ColumnType::Regconfig,
            ColumnType::Regdictionary,
            ColumnType::Regrole,
            ColumnType::Regcollation,
            ColumnType::OidVector,
            ColumnType::Int2Vector,
            ColumnType::TsVector,
            ColumnType::TsQuery,
            ColumnType::Point,
            ColumnType::Lseg,
            ColumnType::Path,
            ColumnType::Box,
            ColumnType::Polygon,
            ColumnType::Line,
            ColumnType::Circle,
            ColumnType::Inet,
            ColumnType::Cidr,
            ColumnType::MacAddr,
            ColumnType::MacAddr8,
            ColumnType::Bit(None),
            ColumnType::VarBit(None),
            ColumnType::Money,
            ColumnType::Oid,
            ColumnType::Xid,
            ColumnType::Xid8,
            ColumnType::Cid,
            ColumnType::Tid,
            ColumnType::PgLsn,
            ColumnType::Json,
            ColumnType::Xml,
            ColumnType::Jsonb,
            ColumnType::JsonPath,
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

    /// A `polygon` column must survive a whole-schema serialize/deserialize, not
    /// just `write_type`/`read_type` in isolation — and the columns around it
    /// must come through untouched, since a new tag that shifted the cursor
    /// would corrupt its neighbours rather than itself.
    #[test]
    fn polygon_column_round_trips_through_a_whole_schema() {
        use assert2::assert;
        let columns = vec![
            Column::new("id", ColumnType::Int4),
            Column::new("label", ColumnType::Text),
            Column::new("region", ColumnType::Polygon),
            Column::new("route", ColumnType::Path),
            Column::new("doc", ColumnType::Jsonb),
            Column::new(
                "tags",
                ColumnType::Array(crabka_pgtypes::ElemType::Varchar(Some(3))),
            ),
        ];
        let bytes = serialize_schema(
            9,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (_, decoded, ..) = deserialize_schema(&bytes).expect("schema reads back");
        assert!(decoded == columns);
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
            column_options: Vec::new(),
        };
        let bytes = serialize_schema(
            table_id,
            &columns,
            TableOptions::default(),
            "postgres",
            Some(&meta),
            None,
            &[],
        );
        let (id, cols, options, _owner, foreign, ..) = deserialize_schema(&bytes).expect("decode");
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
            330_000,
            "kafka_fdw",
            "alice",
            Some("kafka_fdw_handler"),
            Some("kafka_fdw_validator"),
            &[("handler".into(), "kafka_fdw_handler".into())],
        );
        let fdw = deserialize_fdw(&bytes).expect("decode");
        assert_eq!(fdw.oid, 330_000);
        assert_eq!(fdw.name, "kafka_fdw");
        assert_eq!(fdw.owner, "alice");
        assert_eq!(fdw.handler.as_deref(), Some("kafka_fdw_handler"));
        assert_eq!(fdw.validator.as_deref(), Some("kafka_fdw_validator"));
        assert_eq!(fdw.options[0].0, "handler");
    }

    #[test]
    fn roundtrip_server() {
        let bytes = serialize_server(
            330_001,
            "kafka_s",
            "alice",
            "kafka_fdw",
            Some("kafka"),
            Some("1.0"),
            &[("bootstrap".into(), "h:9092".into())],
        );
        let s = deserialize_server(&bytes).expect("decode");
        assert_eq!(s.oid, 330_001);
        assert_eq!(s.name, "kafka_s");
        assert_eq!(s.owner, "alice");
        assert_eq!(s.wrapper, "kafka_fdw");
        assert_eq!(s.server_type.as_deref(), Some("kafka"));
        assert_eq!(s.version.as_deref(), Some("1.0"));
        assert_eq!(s.options[0], ("bootstrap".into(), "h:9092".into()));
    }

    #[test]
    fn roundtrip_user_mapping() {
        let bytes = serialize_user_mapping(
            330_002,
            "alice",
            "kafka_s",
            &[("token".into(), "secret".into())],
        );
        let m = deserialize_user_mapping(&bytes).expect("decode");
        assert_eq!(m.oid, 330_002);
        assert_eq!(m.user, "alice");
        assert_eq!(m.server, "kafka_s");
        assert_eq!(m.options[0], ("token".into(), "secret".into()));
    }

    #[test]
    fn roundtrip_range_type_metadata() {
        let ty = UserType {
            oid: 300_000,
            array_oid: crabka_pgtypes::usertype::user_array_oid(300_000),
            schema: "catalog_types".into(),
            name: "textrange".into(),
            body: UserTypeBody::Range(RangeBody {
                subtype: ColumnType::Text,
                collation: Some("C".into()),
                multirange_schema: Some("multirange_schema".into()),
                multirange_name: Some("multirange_of_text".into()),
            }),
        };
        assert_eq!(deserialize_user_type(&serialize_user_type(&ty)), Ok(ty));
    }

    #[test]
    fn base_type_metadata_roundtrips() {
        let ty = UserType {
            oid: 300_002,
            array_oid: crabka_pgtypes::usertype::user_array_oid(300_002),
            schema: "catalog_types".into(),
            name: "stored_text".into(),
            body: UserTypeBody::Base(BaseBody {
                representation: ColumnType::Text,
                layout: crabka_pgtypes::usertype::BaseLayout::from_representation(ColumnType::Text),
                element: Some(ColumnType::Int4),
                default: Some("'stored default'".into()),
                input: "stored_text_in".into(),
                output: "stored_text_out".into(),
                typmod_in: Some("stored_text_typmodin".into()),
                typmod_out: Some("stored_text_typmodout".into()),
                category: "U".into(),
                preferred: false,
                delimiter: ",".into(),
                storage: 'm',
            }),
        };
        assert_eq!(deserialize_user_type(&serialize_user_type(&ty)), Ok(ty));
    }

    #[test]
    fn domain_constraint_validation_state_roundtrips() {
        let ty = UserType {
            oid: 300_004,
            array_oid: crabka_pgtypes::usertype::user_array_oid(300_004),
            schema: "catalog_types".into(),
            name: "unvalidated_domain".into(),
            body: UserTypeBody::Domain(DomainBody {
                base: ColumnType::Int4,
                not_null: false,
                not_null_name: None,
                default: None,
                checks: vec![DomainCheck {
                    name: "unvalidated_check".into(),
                    expr: "VALUE < 0".into(),
                    validated: false,
                }],
            }),
        };
        assert_eq!(deserialize_user_type(&serialize_user_type(&ty)), Ok(ty));
    }

    #[test]
    fn user_type_identity_roundtrips_dotted_identifiers() {
        let ty = UserType {
            oid: 300_004,
            array_oid: crabka_pgtypes::usertype::user_array_oid(300_004),
            schema: "schema.with.dot".into(),
            name: "type.with.dot".into(),
            body: UserTypeBody::Composite(Vec::new()),
        };
        assert_eq!(deserialize_user_type(&serialize_user_type(&ty)), Ok(ty));
    }

    #[test]
    fn user_type_legacy_records_still_decode() {
        let mut bytes = 300_008u32.to_be_bytes().to_vec();
        write_str(&mut bytes, "catalog_types.legacy_pair");
        bytes.push(USER_TYPE_COMPOSITE);
        write_count(&mut bytes, 0);

        let ty = deserialize_user_type(&bytes).expect("legacy user type decodes");
        assert_eq!(ty.schema, "catalog_types");
        assert_eq!(ty.name, "legacy_pair");
        assert_eq!(ty.body, UserTypeBody::Composite(Vec::new()));
    }

    #[test]
    fn legacy_ranges_keep_their_pre_upgrade_companion_names() {
        for (oid, range_name, companion_name) in [
            (300_012, "textrange", "textmultirange"),
            (300_016, "textrange1", "textrange1_multirange"),
        ] {
            let mut bytes = u32::to_be_bytes(oid).to_vec();
            write_str(&mut bytes, range_name);
            bytes.push(USER_TYPE_RANGE);
            write_type(&mut bytes, ColumnType::Text);
            bytes.push(0); // no collation
            bytes.push(0); // default companion was derived by the old binary

            let ty = deserialize_user_type(&bytes).expect("legacy range decodes");
            assert_eq!(
                ty.multirange_identity(),
                Some(("public".into(), companion_name.into()))
            );
        }
    }

    #[test]
    fn unknown_version_errors() {
        assert!(deserialize_schema(&[1, 0, 0, 0, 0]).is_err());
        assert!(deserialize_schema(&[99, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn ordinary_table_flag_zero_roundtrip() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(
            1,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        let (_, _, options, _owner, foreign, ..) =
            deserialize_schema(&bytes).expect("ordinary table decode");
        assert!(!options.sharded, "ordinary table has no sharded flag");
        assert!(foreign.is_none(), "ordinary table has no foreign meta");
    }

    #[test]
    fn sharded_option_roundtrips() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(
            1,
            &columns,
            TableOptions {
                sharded: true,
                ..TableOptions::default()
            },
            "postgres",
            None,
            None,
            &[],
        );
        let (_, _, options, _owner, foreign, ..) =
            deserialize_schema(&bytes).expect("sharded decode");
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
                key_options: vec![IndexKeyOptions {
                    descending: true,
                    nulls_first: true,
                    opclass: Some("text_pattern_ops".into()),
                    opclass_options: Some("(siglen='1000')".into()),
                    collation: Some("C".into()),
                }],
                include: vec!["name".into()],
                predicate: Some("email IS NOT NULL".into()),
                nulls_not_distinct: true,
                unique: true,
                placement: IndexPlacement::Global,
                method,
                constraint: None,
                without_overlaps: false,
                clustered: false,
                deferral: ConstraintDeferral::Immediate,
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
            key_options: default_index_key_options(2),
            include: Vec::new(),
            predicate: None,
            nulls_not_distinct: false,
            unique: false,
            placement: IndexPlacement::Local,
            method: IndexMethod::Gist,
            constraint: Some(IndexConstraint::Exclusion(vec![
                ExclusionOperator::Equal,
                ExclusionOperator::Overlaps,
            ])),
            without_overlaps: false,
            clustered: false,
            deferral: ConstraintDeferral::Immediate,
        };
        assert_eq!(
            deserialize_index(&serialize_index(&exclusion)).expect("exclusion index decode"),
            exclusion
        );

        // A `PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)` is catalogued as a
        // primary key, not an exclusion constraint, so the temporal flag is the
        // only thing that survives to tell the enforcement path which
        // comparison to use.
        let temporal = Index {
            id: 9,
            name: "temporal_rng_pk".into(),
            table: RelationName::public("temporal_rng"),
            table_id: 5,
            columns: vec!["id".into(), "valid_at".into()],
            key_options: default_index_key_options(2),
            include: Vec::new(),
            predicate: None,
            nulls_not_distinct: false,
            unique: true,
            placement: IndexPlacement::Local,
            method: IndexMethod::Gist,
            constraint: Some(IndexConstraint::PrimaryKey),
            without_overlaps: true,
            clustered: false,
            deferral: ConstraintDeferral::Immediate,
        };
        assert_eq!(
            deserialize_index(&serialize_index(&temporal)).expect("temporal index decode"),
            temporal
        );
    }

    /// `indisclustered` is durable state: it decides which index a later bare
    /// `CLUSTER <table>` reorders by, so it has to survive the wire format in
    /// both settings and not be confused with the flag beside it.
    #[test]
    fn the_clustered_flag_round_trips_independently_of_without_overlaps() {
        use assert2::assert;

        for clustered in [false, true] {
            for without_overlaps in [false, true] {
                let index = Index {
                    id: 11,
                    name: "orders_placed_idx".into(),
                    table: RelationName::public("orders"),
                    table_id: 3,
                    columns: vec!["placed".into()],
                    key_options: default_index_key_options(1),
                    include: Vec::new(),
                    predicate: None,
                    nulls_not_distinct: false,
                    unique: false,
                    placement: IndexPlacement::Local,
                    method: IndexMethod::Btree,
                    constraint: None,
                    without_overlaps,
                    clustered,
                    deferral: ConstraintDeferral::Immediate,
                };
                let decoded =
                    deserialize_index(&serialize_index(&index)).expect("clustered index decode");
                assert!(decoded == index, "{clustered} {without_overlaps}");
            }
        }
    }

    /// A `UNIQUE`/`PRIMARY KEY` constraint's deferrability decides when the key
    /// is checked, so it has to survive the wire format with the three shapes
    /// `condeferrable`/`condeferred` can take and no fourth.
    #[test]
    fn the_constraint_deferral_round_trips_through_the_index_record() {
        use assert2::assert;

        for deferral in [
            ConstraintDeferral::Immediate,
            ConstraintDeferral::Deferrable,
            ConstraintDeferral::Deferred,
        ] {
            let index = Index {
                id: 12,
                name: "unique_tbl_i_key".into(),
                table: RelationName::public("unique_tbl"),
                table_id: 6,
                columns: vec!["i".into()],
                key_options: default_index_key_options(1),
                include: Vec::new(),
                predicate: None,
                nulls_not_distinct: false,
                unique: true,
                placement: IndexPlacement::Local,
                method: IndexMethod::Btree,
                constraint: Some(IndexConstraint::Unique),
                without_overlaps: false,
                clustered: false,
                deferral,
            };
            let decoded =
                deserialize_index(&serialize_index(&index)).expect("deferrable index decode");
            assert!(decoded == index, "{deferral:?}");
        }
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

    /// Neither relation-kind flag may be read loosely: a byte outside the two
    /// each defines comes from a build this one does not understand, and
    /// guessing would hand back a relation of the wrong kind.
    #[test]
    fn unknown_flag_byte_errors() {
        use assert2::assert;

        let columns = vec![Column::new("x", ColumnType::Int4)];
        let matview = MaterializedView {
            definition: String::new(),
            populated: false,
        };
        let plain = serialize_schema(
            1,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            None,
            &[],
        );
        // Setting one payload changes exactly one byte before it appends its
        // own contents, which locates each flag without restating the layout.
        let flag_offset = |set: &[u8]| {
            plain
                .iter()
                .zip(set)
                .position(|(clear, set)| clear != set)
                .expect("a relation-kind payload changes the record")
        };
        let foreign_flag = flag_offset(&serialize_schema(
            1,
            &columns,
            TableOptions::default(),
            "postgres",
            Some(&ForeignTableMeta {
                server: String::new(),
                options: Vec::new(),
                column_options: Vec::new(),
            }),
            None,
            &[],
        ));
        let materialized_flag = flag_offset(&serialize_schema(
            1,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            Some(&matview),
            &[],
        ));
        assert!(foreign_flag != materialized_flag);

        for offset in [foreign_flag, materialized_flag] {
            let mut bytes = plain.clone();
            bytes[offset] = 2;
            assert!(deserialize_schema(&bytes).is_err());
        }
    }

    /// The version byte is what stops a record laid out by another build from
    /// being read as though it had this one's layout. Materialized-view
    /// metadata moved it to `11`, and every other value is refused rather than
    /// decoded on a guess.
    #[test]
    fn the_schema_version_byte_is_current_and_no_other_value_decodes() {
        use assert2::assert;

        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(
            1,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            Some(&MaterializedView {
                definition: "SELECT 1".into(),
                populated: true,
            }),
            &[],
        );
        assert!(bytes[0] == SCHEMA_VERSION);

        for version in u8::MIN..=u8::MAX {
            let mut written = bytes.clone();
            written[0] = version;
            assert!(deserialize_schema(&written).is_ok() == (version == SCHEMA_VERSION));
        }
    }

    /// A record cut short anywhere in the materialized-view payload — mid
    /// definition, before the populated byte — is a corrupt row, not a panic:
    /// the decoder runs on bytes read back from storage, so a short read must
    /// surface as an error the caller can report.
    #[test]
    fn a_truncated_materialized_view_payload_is_a_corrupt_row() {
        use assert2::assert;

        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(
            1,
            &columns,
            TableOptions::default(),
            "postgres",
            None,
            Some(&MaterializedView {
                definition: "SELECT 价格 FROM orders".into(),
                populated: true,
            }),
            &[],
        );

        for cut in 0..bytes.len() {
            assert!(let Err(KvError::CorruptRow(_)) = deserialize_schema(&bytes[..cut]));
        }
        assert!(deserialize_schema(&bytes).is_ok());
    }

    /// The populated byte carries the same strictness as every other flag: a
    /// value outside `0`/`1` would otherwise decode as "populated", which is a
    /// scan of a heap the refresh never filled.
    #[test]
    fn an_unknown_populated_flag_byte_errors() {
        use assert2::assert;

        let columns = vec![Column::new("x", ColumnType::Int4)];
        let encode = |populated| {
            serialize_schema(
                1,
                &columns,
                TableOptions::default(),
                "postgres",
                None,
                Some(&MaterializedView {
                    definition: "SELECT 1".into(),
                    populated,
                }),
                &[],
            )
        };
        let mut bytes = encode(false);
        let populated_offset = bytes
            .iter()
            .zip(encode(true))
            .position(|(clear, set)| *clear != set)
            .expect("a populated matview changes the record");
        bytes[populated_offset] = 2;

        assert!(deserialize_schema(&bytes).is_err());
    }

    /// The two relation-kind payloads occupy separate slots in the record, so a
    /// foreign table decodes with no materialized-view metadata and a
    /// materialized view with no foreign metadata — neither reads the other's
    /// bytes.
    #[test]
    fn foreign_and_materialized_metadata_decode_independently() {
        use assert2::assert;

        let columns = vec![Column::new("x", ColumnType::Int4)];
        let meta = ForeignTableMeta {
            server: "kafka_srv".into(),
            options: vec![("topic".into(), "orders".into())],
            column_options: Vec::new(),
        };
        let matview = MaterializedView {
            definition: "SELECT x FROM orders".into(),
            populated: true,
        };

        for (foreign, materialized) in [
            (None, None),
            (Some(meta.clone()), None),
            (None, Some(matview.clone())),
            (Some(meta), Some(matview)),
        ] {
            let bytes = serialize_schema(
                1,
                &columns,
                TableOptions::default(),
                "postgres",
                foreign.as_ref(),
                materialized.as_ref(),
                &[],
            );

            assert!(
                deserialize_schema(&bytes).expect("decode")
                    == (
                        1,
                        columns.clone(),
                        TableOptions::default(),
                        "postgres".to_string(),
                        foreign,
                        Vec::new(),
                        materialized,
                    )
            );
        }
    }

    /// The reader must refuse an option bit it does not know rather than
    /// treating it as clear: a later version puts row-level security in one of
    /// those bits, and reading it as "off" is a silent security bypass.
    #[test]
    fn unknown_table_option_flags_error() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let encode = |options| serialize_schema(1, &columns, options, "postgres", None, None, &[]);
        let mut bytes = encode(TableOptions::default());
        // The one option the encoder knows changes exactly one byte, which
        // locates the flags without restating the layout here.
        let option_flag_offset = bytes
            .iter()
            .zip(encode(TableOptions {
                sharded: true,
                ..TableOptions::default()
            }))
            .position(|(clear, set)| *clear != set)
            .expect("a set option changes the record");
        bytes[option_flag_offset] = 0b1000_0000;
        assert!(deserialize_schema(&bytes).is_err());
    }

    #[test]
    fn truncated_errors_not_panics() {
        assert!(deserialize_schema(&[SCHEMA_VERSION, 0, 0]).is_err());
    }

    #[test]
    fn roundtrip_view_preserves_owner() {
        use assert2::assert;

        for owner in ["postgres", "regress_view_user", ""] {
            let view = View {
                name: RelationName::public("sales_view"),
                definition: "SELECT 1 AS total, 'x'::text AS label".into(),
                owner: owner.into(),
                columns: vec![
                    Column::new("total", ColumnType::Int4),
                    Column::new("label", ColumnType::Text),
                ],
                options: ViewOptions {
                    security_invoker: true,
                    security_barrier: true,
                    check_option: Some(ViewCheckOption::Cascaded),
                },
            };
            assert!(deserialize_view(&serialize_view(&view)).expect("round trip") == view);
        }
    }

    #[test]
    fn view_record_from_a_superseded_version_is_rejected() {
        use assert2::assert;

        let view = View {
            name: RelationName::public("v"),
            definition: "SELECT 1".into(),
            owner: "postgres".into(),
            columns: Vec::new(),
            options: ViewOptions::default(),
        };
        let mut bytes = serialize_view(&view);
        bytes[0] = VIEW_VERSION - 1;
        assert!(deserialize_view(&bytes).is_err());
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

    #[test]
    fn temporal_precision_round_trips_through_schema_encoding() {
        use assert2::assert;
        use crabka_pgtypes::{ColumnType, IntervalTypmod, TemporalType, datetime::IntervalField};

        let ty = ColumnType::Temporal(TemporalType::Timestamptz, 2);
        let mut bytes = Vec::new();
        super::write_type(&mut bytes, ty);
        assert!(super::read_type(&mut bytes.as_slice()).expect("type") == ty);

        let ty = ColumnType::IntervalTypmod(
            IntervalTypmod::new(IntervalField::Day, IntervalField::Second, Some(3))
                .expect("valid interval typmod"),
        );
        let mut bytes = Vec::new();
        super::write_type(&mut bytes, ty);
        assert!(super::read_type(&mut bytes.as_slice()).expect("type") == ty);
    }
}
