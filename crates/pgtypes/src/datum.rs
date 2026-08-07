//! The runtime value type and the SQL column types of the SP2 slice.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible datum API kept structurally close to donor"
)]

use crate::{
    numeric::{NumericValue, Typmod},
    usertype::{DomainRef, MultirangeRef, RangeRef, UserTypeRef},
};

/// PostgreSQL type OIDs (from pg_type.dat) for the slice's types.
pub mod oids {
    pub const BOOL: u32 = 16;
    /// PostgreSQL `record`: the anonymous composite type a bare `ROW(…)` has.
    pub const RECORD: u32 = 2249;
    /// `record[]`.
    pub const RECORDARRAY: u32 = 2287;
    /// SP40: `bytea`: variable-length binary string.
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    /// PostgreSQL `smallint`: a 2-byte signed integer.
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    /// PostgreSQL `oid` — object identifier, an **unsigned** 4-byte integer.
    pub const OID: u32 = 26;
    /// `oid[]`.
    pub const OIDARRAY: u32 = 1028;
    /// PostgreSQL `xid` — a 32-bit transaction id. Equality only: transaction
    /// ids compare with modular arithmetic, which has no total order.
    pub const XID: u32 = 28;
    /// `xid[]`.
    pub const XIDARRAY: u32 = 1011;
    /// PostgreSQL `xid8` — a 64-bit (epoch-extended) transaction id, fully
    /// ordered unlike its 32-bit sibling.
    pub const XID8: u32 = 5069;
    /// `xid8[]`.
    pub const XID8ARRAY: u32 = 271;
    /// PostgreSQL `cid` — a 32-bit command id. Equality only.
    pub const CID: u32 = 29;
    /// `cid[]`.
    pub const CIDARRAY: u32 = 1012;
    /// PostgreSQL `tid` — a `(block, offset)` tuple identifier.
    pub const TID: u32 = 27;
    /// `tid[]`.
    pub const TIDARRAY: u32 = 1010;
    /// PostgreSQL `pg_lsn` — a write-ahead log sequence number, printed `X/Y`
    /// in hexadecimal.
    pub const PG_LSN: u32 = 3220;
    /// `pg_lsn[]`.
    pub const PG_LSNARRAY: u32 = 3221;
    pub const OIDVECTOR: u32 = 30;
    /// PostgreSQL `int2vector` — a zero-based `int2` array with the same
    /// space-separated text form `oidvector` uses.
    pub const INT2VECTOR: u32 = 22;
    /// PostgreSQL `regclass` — a relation's `pg_class` oid with name-based
    /// text input; values live in the `Int4` datum like `oid`.
    pub const REGCLASS: u32 = 2205;
    pub const REGTYPE: u32 = 2206;
    pub const REGPROCEDURE: u32 = 2202;
    /// PostgreSQL `regnamespace` — a schema's `pg_namespace` oid rendered as
    /// its name. `psql`'s `\d` casts `stxnamespace` through it.
    pub const REGNAMESPACE: u32 = 4089;
    pub const REGNAMESPACEARRAY: u32 = 4090;
    pub const REGTYPEARRAY: u32 = 2211;
    /// PostgreSQL `regproc` — a `pg_proc` oid rendered as the bare function
    /// name, without its argument types. Unlike every other member of the
    /// family its oid is in the bootstrap band, because `pg_proc.protransform`
    /// and friends are declared with it.
    pub const REGPROC: u32 = 24;
    /// PostgreSQL `regoper` — a `pg_operator` oid rendered as the bare operator
    /// name.
    pub const REGOPER: u32 = 2203;
    /// PostgreSQL `regoperator` — a `pg_operator` oid rendered with its operand
    /// types, `NONE` standing in for the missing side of a unary operator.
    pub const REGOPERATOR: u32 = 2204;
    /// PostgreSQL `regconfig` — a `pg_ts_config` oid.
    pub const REGCONFIG: u32 = 3734;
    /// PostgreSQL `regdictionary` — a `pg_ts_dict` oid.
    pub const REGDICTIONARY: u32 = 3769;
    /// PostgreSQL `regrole` — a `pg_authid` oid. Roles are cluster-wide, so it
    /// is one of the two members that never schema-qualifies.
    pub const REGROLE: u32 = 4096;
    /// PostgreSQL `regcollation` — a `pg_collation` oid.
    pub const REGCOLLATION: u32 = 4191;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    /// PostgreSQL `real`: single-precision IEEE-754 (`f32`).
    pub const FLOAT4: u32 = 700;
    /// SP30: `double precision` (IEEE-754 f64).
    pub const FLOAT8: u32 = 701;
    /// PostgreSQL geometric point.
    pub const POINT: u32 = 600;
    /// PostgreSQL geometric path.
    pub const PATH: u32 = 602;
    /// PostgreSQL `lseg` — a line segment between two points.
    pub const LSEG: u32 = 601;
    /// PostgreSQL `line` — an infinite line as `Ax + By + C = 0`.
    pub const LINE: u32 = 628;
    /// PostgreSQL `circle` — a centre point and a radius.
    pub const CIRCLE: u32 = 718;
    /// PostgreSQL `box` — an axis-aligned rectangle.
    pub const BOX: u32 = 603;
    /// SP32: arbitrary-precision `numeric`/`decimal`.
    pub const NUMERIC: u32 = 1700;
    /// SP37: `date`: days since 2000-01-01, stored as i32.
    pub const DATE: u32 = 1082;
    /// SP37: `time without time zone`: microseconds since midnight, stored as i64.
    pub const TIME: u32 = 1083;
    /// `time with time zone`: microseconds since midnight plus a UTC offset.
    pub const TIMETZ: u32 = 1266;
    /// SP37: `timestamp without time zone`: microseconds since 2000-01-01 00:00:00.
    pub const TIMESTAMP: u32 = 1114;
    /// SP37: `timestamp with time zone`: microseconds since Unix epoch (UTC), stored as i64.
    pub const TIMESTAMPTZ: u32 = 1184;
    /// SP37: `interval`: months (i32) + days (i32) + microseconds (i64), stored as 16 bytes.
    pub const INTERVAL: u32 = 1186;
    /// PostgreSQL `uuid`: 128-bit universally unique identifier.
    pub const UUID: u32 = 2950;
    /// PostgreSQL `json`: accepted on input (parameters, casts) as an alias for
    /// `jsonb`; crabka never *reports* this OID.
    pub const JSON: u32 = 114;
    /// PostgreSQL `jsonb`: the decomposed binary JSON type.
    pub const JSONB: u32 = 3802;
    /// PostgreSQL `jsonpath` — a parsed SQL/JSON path expression.
    pub const JSONPATH: u32 = 4072;
    /// PostgreSQL `jsonpath[]`.
    pub const JSONPATHARRAY: u32 = 4073;
    pub const INT4RANGE: u32 = 3904;
    pub const INT4RANGEARRAY: u32 = 3905;
    pub const NUMRANGE: u32 = 3906;
    pub const NUMRANGEARRAY: u32 = 3907;
    pub const TSRANGE: u32 = 3908;
    pub const TSRANGEARRAY: u32 = 3909;
    pub const TSTZRANGE: u32 = 3910;
    pub const TSTZRANGEARRAY: u32 = 3911;
    pub const DATERANGE: u32 = 3912;
    pub const DATERANGEARRAY: u32 = 3913;
    pub const INT8RANGE: u32 = 3926;
    pub const INT8RANGEARRAY: u32 = 3927;
    pub const INT4MULTIRANGE: u32 = 4451;
    pub const NUMMULTIRANGE: u32 = 4532;
    pub const TSMULTIRANGE: u32 = 4533;
    pub const TSTZMULTIRANGE: u32 = 4534;
    pub const DATEMULTIRANGE: u32 = 4535;
    pub const INT8MULTIRANGE: u32 = 4536;
    pub const INT4MULTIRANGEARRAY: u32 = 6150;
    pub const NUMMULTIRANGEARRAY: u32 = 6151;
    pub const TSMULTIRANGEARRAY: u32 = 6152;
    pub const TSTZMULTIRANGEARRAY: u32 = 6153;
    pub const DATEMULTIRANGEARRAY: u32 = 6155;
    pub const INT8MULTIRANGEARRAY: u32 = 6157;
    /// `json[]`.
    pub const JSONARRAY: u32 = 199;
    /// `jsonb[]`.
    pub const JSONBARRAY: u32 = 3807;
    /// `boolean[]`.
    pub const BOOLARRAY: u32 = 1000;
    /// `bytea[]`.
    pub const BYTEAARRAY: u32 = 1001;
    /// `smallint[]`.
    pub const INT2ARRAY: u32 = 1005;
    /// `bigint[]`.
    pub const INT8ARRAY: u32 = 1016;
    /// `real[]`.
    pub const FLOAT4ARRAY: u32 = 1021;
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
    /// `character(n)[]`.
    pub const BPCHARARRAY: u32 = 1014;
    /// `character varying(n)[]`.
    pub const VARCHARARRAY: u32 = 1015;
    /// PostgreSQL `tsvector` and its array type.
    pub const TSVECTOR: u32 = 3614;
    pub const TSVECTORARRAY: u32 = 3643;
    /// PostgreSQL `tsquery` and its array type.
    pub const TSQUERY: u32 = 3615;
    pub const TSQUERYARRAY: u32 = 3645;
    /// PostgreSQL `inet` — a host address with an optional netmask.
    pub const INET: u32 = 869;
    /// `inet[]`.
    pub const INETARRAY: u32 = 1041;
    /// PostgreSQL `cidr` — a network address, sharing `inet`'s representation.
    pub const CIDR: u32 = 650;
    /// `cidr[]`.
    pub const CIDRARRAY: u32 = 651;
    /// PostgreSQL `macaddr` — a six-byte EUI-48 hardware address.
    pub const MACADDR: u32 = 829;
    /// `macaddr[]`.
    pub const MACADDRARRAY: u32 = 1040;
    /// PostgreSQL `macaddr8` — an eight-byte EUI-64 hardware address.
    pub const MACADDR8: u32 = 774;
    /// `macaddr8[]`.
    pub const MACADDR8ARRAY: u32 = 775;
    /// PostgreSQL `money` — a 64-bit count of minor currency units.
    pub const MONEY: u32 = 790;
    /// `money[]`.
    pub const MONEYARRAY: u32 = 791;
    /// PostgreSQL `bit` — a fixed-length string of bits.
    pub const BIT: u32 = 1560;
    /// `bit[]`.
    pub const BITARRAY: u32 = 1561;
    /// PostgreSQL `bit varying` (`varbit`).
    pub const VARBIT: u32 = 1562;
    /// `varbit[]`.
    pub const VARBITARRAY: u32 = 1563;
}

/// The element type of a SQL array.
///
/// This is a separate `Copy` enum rather than a boxed [`ColumnType`], because
/// the executor passes `ColumnType` by value everywhere. It is deliberately
/// smaller than `ColumnType`: `regclass` and arrays themselves have no array
/// form here, and [`ElemType::from_column_type`] refuses them with 0A000.
/// `PostgreSQL` has no nested array type: `int[][]` *is* `int[]` (`_int4`), and
/// the extra dimensions live in the value, not the type.
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
    Json,
    Jsonb,
    JsonPath,
    Int2,
    Float4,
    Regtype,
    /// `character varying(n)[]` — the length modifier is applied to each element
    /// on assignment, exactly as `PostgreSQL` applies it to a scalar `varchar(n)`.
    Varchar(Option<u16>),
    /// `character(n)[]`: each element is blank-padded to `n` on assignment.
    Char(Option<u16>),
    Range(RangeRef),
    Multirange(MultirangeRef),
}

impl ElemType {
    /// Every supported array element type, in `code()` order. The two
    /// length-modified entries stand for their whole family — `from_code`
    /// reconstructs the modifier, and neither the OID nor the name depends on it.
    pub const ALL: [ElemType; 21] = [
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
        ElemType::Int2,
        ElemType::Float4,
        ElemType::Varchar(None),
        ElemType::Char(None),
        ElemType::Regtype,
        ElemType::JsonPath,
        ElemType::Json,
    ];

    /// The element type as a column type (`numeric` is unconstrained, because an
    /// array carries no element typmod).
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
            ElemType::Json => ColumnType::Json,
            ElemType::Jsonb => ColumnType::Jsonb,
            ElemType::JsonPath => ColumnType::JsonPath,
            ElemType::Int2 => ColumnType::Int2,
            ElemType::Float4 => ColumnType::Float4,
            ElemType::Regtype => ColumnType::Regtype,
            ElemType::Varchar(n) => ColumnType::Varchar(n),
            ElemType::Char(n) => ColumnType::Char(n),
            ElemType::Range(range) => ColumnType::Range(range),
            ElemType::Multirange(multirange) => ColumnType::Multirange(multirange),
        }
    }

    /// The element type for `elem`, or `None` when crabka has no array type for
    /// it (`regclass`, and the array types themselves, because `PostgreSQL` has
    /// no nested array type, so `int[][]` resolves to `int[]` at the type
    /// level).
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
            // `timetz` has no array type in crabka yet.
            ColumnType::Timetz => return None,
            ColumnType::Timestamp => ElemType::Timestamp,
            ColumnType::Timestamptz => ElemType::Timestamptz,
            ColumnType::Interval => ElemType::Interval,
            ColumnType::Bytea => ElemType::Bytea,
            ColumnType::Uuid => ElemType::Uuid,
            ColumnType::Json => ElemType::Json,
            ColumnType::Jsonb => ElemType::Jsonb,
            ColumnType::JsonPath => ElemType::JsonPath,
            ColumnType::Int2 => ElemType::Int2,
            ColumnType::Float4 => ElemType::Float4,
            ColumnType::Varchar(n) => ElemType::Varchar(n),
            ColumnType::Char(n) => ElemType::Char(n),
            // Composite, enum and domain element types are not supported: an
            // array of them would need an element oid the array encoder cannot
            // name, so callers report 0A000 rather than mis-encoding.
            ColumnType::Point
            | ColumnType::Path
            | ColumnType::Lseg
            | ColumnType::Line
            | ColumnType::Circle
            | ColumnType::Box
            | ColumnType::Regclass
            | ColumnType::Regprocedure
            | ColumnType::Regnamespace
            | ColumnType::Regproc
            | ColumnType::Regoper
            | ColumnType::Regoperator
            | ColumnType::Regconfig
            | ColumnType::Regdictionary
            | ColumnType::Regrole
            | ColumnType::Regcollation
            | ColumnType::OidVector
            | ColumnType::Int2Vector
            | ColumnType::TsVector
            | ColumnType::TsQuery
            // The network types have no array element form here for the same
            // reason the geometric ones do not: their array oids would need an
            // `ElemType` the array encoder cannot name.
            | ColumnType::Inet
            | ColumnType::Cidr
            | ColumnType::MacAddr
            | ColumnType::MacAddr8
            // The system identifier types have no `ElemType` for the same
            // reason: their array oids would need an element the array encoder
            // cannot name.
            | ColumnType::Oid
            | ColumnType::Xid
            | ColumnType::Xid8
            | ColumnType::Cid
            | ColumnType::Tid
            | ColumnType::PgLsn
            | ColumnType::Money
            | ColumnType::Bit(_)
            | ColumnType::VarBit(_)
            | ColumnType::Array(_)
            | ColumnType::Record(_)
            | ColumnType::Enum(_)
            | ColumnType::Domain(_) => return None,
            ColumnType::Range(range) => {
                let elem = ElemType::Range(range);
                if elem.array_oid() == 0 {
                    return None;
                }
                elem
            }
            ColumnType::Multirange(multirange) => ElemType::Multirange(multirange),
            ColumnType::Regtype => ElemType::Regtype,
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
            ElemType::Json => oids::JSONARRAY,
            ElemType::Jsonb => oids::JSONBARRAY,
            ElemType::JsonPath => oids::JSONPATHARRAY,
            ElemType::Int2 => oids::INT2ARRAY,
            ElemType::Float4 => oids::FLOAT4ARRAY,
            ElemType::Regtype => oids::REGTYPEARRAY,
            ElemType::Varchar(_) => oids::VARCHARARRAY,
            ElemType::Char(_) => oids::BPCHARARRAY,
            ElemType::Range(range) => match range.oid {
                oids::INT4RANGE => oids::INT4RANGEARRAY,
                oids::NUMRANGE => oids::NUMRANGEARRAY,
                oids::TSRANGE => oids::TSRANGEARRAY,
                oids::TSTZRANGE => oids::TSTZRANGEARRAY,
                oids::DATERANGE => oids::DATERANGEARRAY,
                oids::INT8RANGE => oids::INT8RANGEARRAY,
                _ => 0,
            },
            ElemType::Multirange(multirange) => match multirange.oid {
                oids::INT4MULTIRANGE => oids::INT4MULTIRANGEARRAY,
                oids::NUMMULTIRANGE => oids::NUMMULTIRANGEARRAY,
                oids::TSMULTIRANGE => oids::TSMULTIRANGEARRAY,
                oids::TSTZMULTIRANGE => oids::TSTZMULTIRANGEARRAY,
                oids::DATEMULTIRANGE => oids::DATEMULTIRANGEARRAY,
                oids::INT8MULTIRANGE => oids::INT8MULTIRANGEARRAY,
                oid => crate::usertype::user_multirange_array_oid(oid),
            },
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
            ElemType::Json => "json[]",
            ElemType::Jsonb => "jsonb[]",
            ElemType::JsonPath => "jsonpath[]",
            ElemType::Int2 => "smallint[]",
            ElemType::Float4 => "real[]",
            ElemType::Regtype => "regtype[]",
            ElemType::Varchar(_) => "character varying[]",
            ElemType::Char(_) => "character[]",
            ElemType::Range(range) => match range.oid {
                oids::INT4RANGE => "int4range[]",
                oids::NUMRANGE => "numrange[]",
                oids::TSRANGE => "tsrange[]",
                oids::TSTZRANGE => "tstzrange[]",
                oids::DATERANGE => "daterange[]",
                oids::INT8RANGE => "int8range[]",
                _ => "range[]",
            },
            ElemType::Multirange(multirange) => match multirange.oid {
                oids::INT4MULTIRANGE => "int4multirange[]",
                oids::NUMMULTIRANGE => "nummultirange[]",
                oids::TSMULTIRANGE => "tsmultirange[]",
                oids::TSTZMULTIRANGE => "tstzmultirange[]",
                oids::DATEMULTIRANGE => "datemultirange[]",
                oids::INT8MULTIRANGE => "int8multirange[]",
                _ => "multirange[]",
            },
        }
    }

    /// The length modifier this element type carries, when it has one.
    pub fn typmod(self) -> Option<u16> {
        match self {
            ElemType::Varchar(n) | ElemType::Char(n) => n,
            _ => None,
        }
    }

    /// A stable, **append-only** wire/storage code. The row encoder and the
    /// catalog's schema serializer persist it, so existing values must never
    /// change. It does **not** carry the length modifier of
    /// `varchar(n)`/`char(n)`. Use [`ElemType::write_code`] /
    /// [`ElemType::read_code`] for a lossless round trip.
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
            ElemType::Int2 => 14,
            ElemType::Float4 => 15,
            ElemType::Varchar(_) => 16,
            ElemType::Char(_) => 17,
            ElemType::Range(_) => 18,
            ElemType::Multirange(_) => 19,
            ElemType::Regtype => 20,
            ElemType::JsonPath => 21,
            ElemType::Json => 22,
        }
    }

    /// The inverse of [`ElemType::code`] (`None` for an unknown code). The
    /// length-modified families come back unconstrained.
    pub fn from_code(code: u8) -> Option<Self> {
        ElemType::ALL.into_iter().find(|e| e.code() == code)
    }

    /// Append the lossless storage encoding: the [`ElemType::code`] byte, plus a
    /// present-flag and a big-endian `u16` for the length-modified families.
    pub fn write_code(self, out: &mut Vec<u8>) {
        out.push(self.code());
        if let ElemType::Range(range) = self {
            out.extend_from_slice(&range.oid.to_be_bytes());
        } else if let ElemType::Multirange(multirange) = self {
            out.extend_from_slice(&multirange.oid.to_be_bytes());
        } else if matches!(self, ElemType::Varchar(_) | ElemType::Char(_)) {
            match self.typmod() {
                None => out.push(0),
                Some(n) => {
                    out.push(1);
                    out.extend_from_slice(&n.to_be_bytes());
                }
            }
        }
    }

    /// The inverse of [`ElemType::write_code`], advancing `cursor` past the
    /// bytes it consumed. `None` means the bytes are not a valid encoding.
    pub fn read_code(cursor: &mut &[u8]) -> Option<Self> {
        let (code, rest) = cursor.split_first()?;
        *cursor = rest;
        if *code == 18 {
            let (bytes, rest) = cursor.split_at_checked(4)?;
            *cursor = rest;
            let oid = u32::from_be_bytes(bytes.try_into().ok()?);
            return match ColumnType::builtin_range(oid)
                .or_else(|| crate::usertype::lookup_oid(oid).map(|ty| ty.column_type()))?
            {
                ColumnType::Range(range) => Some(ElemType::Range(range)),
                _ => None,
            };
        }
        if *code == 19 {
            let (bytes, rest) = cursor.split_at_checked(4)?;
            *cursor = rest;
            let oid = u32::from_be_bytes(bytes.try_into().ok()?);
            return match ColumnType::builtin_multirange(oid)
                .or_else(|| crate::usertype::column_type_for_oid(oid))?
            {
                ColumnType::Multirange(multirange) => Some(ElemType::Multirange(multirange)),
                _ => None,
            };
        }
        let base = ElemType::from_code(*code)?;
        if !matches!(base, ElemType::Varchar(_) | ElemType::Char(_)) {
            return Some(base);
        }
        let (present, rest) = cursor.split_first()?;
        *cursor = rest;
        let limit = match present {
            0 => None,
            1 => {
                let (bytes, rest) = cursor.split_at_checked(2)?;
                *cursor = rest;
                Some(u16::from_be_bytes([bytes[0], bytes[1]]))
            }
            _ => return None,
        };
        Some(match base {
            ElemType::Char(_) => ElemType::Char(limit),
            _ => ElemType::Varchar(limit),
        })
    }

    /// The element type of an array OID (`pg_type.typelem`), for parameter
    /// binding.
    pub fn from_array_oid(oid: u32) -> Option<Self> {
        for range_oid in [
            oids::INT4RANGE,
            oids::NUMRANGE,
            oids::TSRANGE,
            oids::TSTZRANGE,
            oids::DATERANGE,
            oids::INT8RANGE,
        ] {
            let range = match ColumnType::builtin_range(range_oid)? {
                ColumnType::Range(range) => range,
                _ => unreachable!(),
            };
            let elem = ElemType::Range(range);
            if elem.array_oid() == oid {
                return Some(elem);
            }
        }
        for multirange_oid in [
            oids::INT4MULTIRANGE,
            oids::NUMMULTIRANGE,
            oids::TSMULTIRANGE,
            oids::TSTZMULTIRANGE,
            oids::DATEMULTIRANGE,
            oids::INT8MULTIRANGE,
        ] {
            let multirange = match ColumnType::builtin_multirange(multirange_oid)? {
                ColumnType::Multirange(multirange) => multirange,
                _ => unreachable!(),
            };
            let elem = ElemType::Multirange(multirange);
            if elem.array_oid() == oid {
                return Some(elem);
            }
        }
        if let Some(multirange) = crate::usertype::all().into_iter().find_map(|ty| {
            let ColumnType::Multirange(multirange) = ty.multirange_type()? else {
                return None;
            };
            (ElemType::Multirange(multirange).array_oid() == oid).then_some(multirange)
        }) {
            return Some(ElemType::Multirange(multirange));
        }
        ElemType::ALL.into_iter().find(|e| e.array_oid() == oid)
    }
}

/// A SQL column type. SP30 added `Float8`; SP32 added `Numeric`, which carries
/// an optional `numeric(precision, scale)` modifier for column definitions and
/// casts, where `None` is unconstrained `numeric`. SP37 adds five date/time
/// types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    /// PostgreSQL `smallint` (OID 21): a 2-byte signed integer.
    Int2,
    Int4,
    Int8,
    Text,
    Varchar(Option<u16>),
    Char(Option<u16>),
    /// PostgreSQL `real` (OID 700): an IEEE-754 `f32`.
    Float4,
    /// SP30: PostgreSQL `double precision` (an IEEE-754 `f64`).
    Float8,
    /// PostgreSQL `point` (OID 600): two double-precision coordinates.
    Point,
    /// PostgreSQL `path` (OID 602): an open or closed point sequence.
    Path,
    /// PostgreSQL `lseg` (OID 601): a line segment between two endpoints.
    Lseg,
    /// PostgreSQL `line` (OID 628): the infinite line `Ax + By + C = 0`.
    Line,
    /// PostgreSQL `circle` (OID 718): a centre point and a radius.
    Circle,
    /// PostgreSQL `box` (OID 603): an axis-aligned rectangle.
    Box,
    /// SP32: PostgreSQL `numeric`/`decimal`. The `Typmod` (precision, scale) is
    /// significant only when storing/casting; OID/name/typlen ignore it.
    Numeric(Option<Typmod>),
    /// SP37: PostgreSQL `date` (OID 1082): a calendar date with no time-of-day.
    Date,
    /// SP37: PostgreSQL `time without time zone` (OID 1083).
    Time,
    /// PostgreSQL `time with time zone` (OID 1266): a clock reading plus the
    /// UTC offset it was read at.
    Timetz,
    /// SP37: PostgreSQL `timestamp without time zone` (OID 1114).
    Timestamp,
    /// SP37: PostgreSQL `timestamp with time zone` (OID 1184): stored as UTC.
    Timestamptz,
    /// SP37: PostgreSQL `interval` (OID 1186): months + days + microseconds.
    Interval,
    /// SP40: PostgreSQL `bytea` (OID 17): variable-length binary string.
    Bytea,
    /// PostgreSQL `uuid` (OID 2950): 128-bit identifier.
    Uuid,
    /// PostgreSQL `regclass` (OID 2205): a relation's `pg_class` oid. Values
    /// are `Datum::Int4` like `oid`; what distinguishes the type is input
    /// conversion (a non-numeric string is a relation name that needs catalog
    /// resolution, which the session/executor layers do, because the pure
    /// datum-parse path only accepts numeric strings).
    Regclass,
    /// PostgreSQL `regtype` (OID 2206), represented by the shared named-oid
    /// datum because its comparison and wire identity are the oid.
    Regtype,
    /// PostgreSQL `regprocedure` (OID 2202), a `pg_proc` oid rendered with its
    /// identity argument types.
    Regprocedure,
    /// PostgreSQL `regnamespace` (OID 4089), a `pg_namespace` oid rendered as
    /// the schema's name. Values share `Datum::Regclass` with the other reg
    /// types because comparison and wire identity are the oid.
    Regnamespace,
    /// PostgreSQL `regproc` (OID 24), a `pg_proc` oid rendered as the bare
    /// function name. It differs from [`Regprocedure`](Self::Regprocedure) in
    /// that a name matching more than one overload is 42725 rather than a
    /// resolution.
    Regproc,
    /// PostgreSQL `regoper` (OID 2203), a `pg_operator` oid rendered as the
    /// bare operator name.
    Regoper,
    /// PostgreSQL `regoperator` (OID 2204), a `pg_operator` oid rendered with
    /// its operand types.
    Regoperator,
    /// PostgreSQL `regconfig` (OID 3734), a `pg_ts_config` oid.
    Regconfig,
    /// PostgreSQL `regdictionary` (OID 3769), a `pg_ts_dict` oid.
    Regdictionary,
    /// PostgreSQL `regrole` (OID 4096), a `pg_authid` oid.
    Regrole,
    /// PostgreSQL `regcollation` (OID 4191), a `pg_collation` oid.
    Regcollation,
    /// PostgreSQL `oidvector` (OID 30), an oid array with lower bound zero and
    /// a space-separated text representation.
    OidVector,
    /// PostgreSQL `int2vector` (OID 22), a zero-based `int2` array sharing
    /// `oidvector`'s space-separated text form.
    Int2Vector,
    /// PostgreSQL's normalized full-text document and query types.
    TsVector,
    TsQuery,
    /// PostgreSQL `inet` (OID 869) — a host address plus an optional netmask.
    Inet,
    /// PostgreSQL `cidr` (OID 650) — a network address. Shares `inet`'s
    /// representation and values; what differs is the input check (no bits set
    /// to the right of the netmask) and the output function.
    Cidr,
    /// PostgreSQL `macaddr` (OID 829) — a six-byte EUI-48 address.
    MacAddr,
    /// PostgreSQL `macaddr8` (OID 774) — an eight-byte EUI-64 address.
    MacAddr8,
    /// PostgreSQL `oid` (OID 26) — an **unsigned** 32-bit object identifier.
    /// Not `int4` under another name: real catalog oids exceed 2^31, and
    /// `oidin` reads `-1` as 4294967295 rather than rejecting it.
    Oid,
    /// PostgreSQL `xid` (OID 28) — a 32-bit transaction id. It shares `oid`'s
    /// input function and width but has **only** `=` and `<>`, because
    /// transaction ids compare with modular arithmetic.
    Xid,
    /// PostgreSQL `xid8` (OID 5069) — a 64-bit transaction id, which unlike
    /// `xid` is fully ordered and has both a B-tree and a hash opclass.
    Xid8,
    /// PostgreSQL `cid` (OID 29) — a 32-bit command id, with `=` and nothing
    /// else (not even `<>`).
    Cid,
    /// PostgreSQL `tid` (OID 27) — a `(block, offset)` tuple identifier.
    Tid,
    /// PostgreSQL `pg_lsn` (OID 3220) — a 64-bit log sequence number written
    /// `X/Y` in hexadecimal.
    PgLsn,
    /// PostgreSQL `money` (OID 790) — a signed 64-bit count of minor currency
    /// units, rendered through `lc_monetary`.
    Money,
    /// PostgreSQL `bit` (OID 1560) — a fixed-length bit string. The modifier is
    /// the declared length; `None` is the unconstrained type, which `bit_in`
    /// treats as "however many bits the input has" and which every other path
    /// treats as `bit(1)`, exactly as PostgreSQL does.
    Bit(Option<i32>),
    /// PostgreSQL `bit varying` (OID 1562) — a bit string with an optional
    /// maximum length. Shares `bit`'s values: the two are binary-coercible in
    /// both directions.
    VarBit(Option<i32>),
    /// PostgreSQL `json` (OID 114) — the input text, validated and otherwise
    /// untouched, so whitespace, object key order and duplicate keys all survive
    /// a round trip. `jsonb` is the decomposed sibling; the two are different
    /// types, not two spellings of one.
    Json,
    /// PostgreSQL `jsonb` (OID 3802) — decomposed JSON: whitespace dropped,
    /// numbers held as `numeric`, object keys sorted and de-duplicated.
    Jsonb,
    /// PostgreSQL `jsonpath` (OID 4072). Values use the existing canonical text
    /// datum; the executor validates and normalizes them at input boundaries.
    JsonPath,
    /// A one-dimensional PostgreSQL array (OID = the element type's `typarray`).
    Array(ElemType),
    /// A composite type: the anonymous `record` (OID 2249) that a bare `ROW(…)`
    /// has, or a named composite created by `CREATE TYPE … AS (…)`.
    Record(Option<UserTypeRef>),
    /// `CREATE TYPE … AS ENUM (…)`.
    Enum(UserTypeRef),
    /// A built-in or user-defined range type.
    Range(RangeRef),
    /// A canonical ordered set of non-overlapping ranges.
    Multirange(MultirangeRef),
    /// `CREATE DOMAIN … AS base` — a base type plus constraints. Values are the
    /// base type's values; what the domain adds is the constraint check on
    /// assignment and cast, and the type it reports.
    Domain(DomainRef),
}

impl ColumnType {
    /// Resolve a built-in PostgreSQL range oid.
    #[must_use]
    pub fn builtin_range(oid: u32) -> Option<Self> {
        let (name, subtype) = match oid {
            oids::INT4RANGE => ("int4range", &ColumnType::Int4),
            oids::NUMRANGE => ("numrange", &ColumnType::Numeric(None)),
            oids::TSRANGE => ("tsrange", &ColumnType::Timestamp),
            oids::TSTZRANGE => ("tstzrange", &ColumnType::Timestamptz),
            oids::DATERANGE => ("daterange", &ColumnType::Date),
            oids::INT8RANGE => ("int8range", &ColumnType::Int8),
            _ => return None,
        };
        Some(ColumnType::Range(RangeRef { oid, name, subtype }))
    }

    #[must_use]
    pub fn builtin_multirange(oid: u32) -> Option<Self> {
        let (name, range_oid) = match oid {
            oids::INT4MULTIRANGE => ("int4multirange", oids::INT4RANGE),
            oids::NUMMULTIRANGE => ("nummultirange", oids::NUMRANGE),
            oids::TSMULTIRANGE => ("tsmultirange", oids::TSRANGE),
            oids::TSTZMULTIRANGE => ("tstzmultirange", oids::TSTZRANGE),
            oids::DATEMULTIRANGE => ("datemultirange", oids::DATERANGE),
            oids::INT8MULTIRANGE => ("int8multirange", oids::INT8RANGE),
            _ => return None,
        };
        let ColumnType::Range(range) = Self::builtin_range(range_oid)? else {
            unreachable!()
        };
        Some(ColumnType::Multirange(MultirangeRef { oid, name, range }))
    }

    /// Resolve the automatic multirange companion for a built-in or registered range.
    #[must_use]
    pub fn multirange_for_range(range: RangeRef) -> Option<Self> {
        let oid = match range.oid {
            oids::INT4RANGE => oids::INT4MULTIRANGE,
            oids::NUMRANGE => oids::NUMMULTIRANGE,
            oids::TSRANGE => oids::TSMULTIRANGE,
            oids::TSTZRANGE => oids::TSTZMULTIRANGE,
            oids::DATERANGE => oids::DATEMULTIRANGE,
            oids::INT8RANGE => oids::INT8MULTIRANGE,
            oid => return crate::usertype::column_type_for_oid(oid.checked_add(3)?),
        };
        Self::builtin_multirange(oid)
    }

    /// Resolve a bare SQL type name (no modifier). `numeric`/`decimal` resolve to
    /// the unconstrained form; the parser layers the `(p, s)` modifier on top.
    pub fn from_sql_name(name: &str) -> Option<Self> {
        Self::from_builtin_sql_name(&name.to_ascii_lowercase())
            .or_else(|| crate::usertype::column_type_for_name(name))
    }

    /// Resolve only a built-in SQL type name. Session-aware parsers use this
    /// while walking `pg_catalog` in search-path order, then consult exact user
    /// type identities for every other visible schema.
    #[must_use]
    pub fn from_builtin_sql_name(name: &str) -> Option<Self> {
        match name {
            "int2" | "smallint" => Some(ColumnType::Int2),
            "int4" | "integer" | "int" => Some(ColumnType::Int4),
            "int8" | "bigint" => Some(ColumnType::Int8),
            "oid" => Some(ColumnType::Oid),
            "xid" => Some(ColumnType::Xid),
            "xid8" => Some(ColumnType::Xid8),
            "cid" => Some(ColumnType::Cid),
            "tid" => Some(ColumnType::Tid),
            "pg_lsn" => Some(ColumnType::PgLsn),
            // `name` (OID 19) is a pragmatic alias for `text`, the same shape of
            // divergence as `oid` → `int4` above: the catalog's name-valued
            // columns are already Text, so `'x'::name` and a `name[]` column
            // resolve consistently with them. RowDescription therefore reports
            // text (25), not name (19), and the 63-byte truncation is not applied.
            "text" | "name" => Some(ColumnType::Text),
            "varchar" | "character varying" => Some(ColumnType::Varchar(None)),
            // `bpchar` is PostgreSQL's own internal name for the blank-padded
            // character type, and it is accepted as a type name: `'ab'::bpchar`
            // works. Unlike `char`/`character` it carries no implicit (1).
            "char" | "character" => Some(ColumnType::Char(Some(1))),
            "bpchar" => Some(ColumnType::Char(None)),
            "bool" | "boolean" => Some(ColumnType::Bool),
            // SP30: `float` (no precision) is `double precision` in PostgreSQL; the
            // two-word `double precision` is normalized to this single string by the
            // parser before it reaches here.
            "float8" | "float" | "double precision" => Some(ColumnType::Float8),
            "point" => Some(ColumnType::Point),
            "path" => Some(ColumnType::Path),
            "lseg" => Some(ColumnType::Lseg),
            "line" => Some(ColumnType::Line),
            "circle" => Some(ColumnType::Circle),
            "box" => Some(ColumnType::Box),
            "float4" | "real" => Some(ColumnType::Float4),
            // SP32: `numeric`/`decimal` (unconstrained here; typmod added by parser).
            "numeric" | "decimal" => Some(ColumnType::Numeric(None)),
            // SP37: date/time types. `timetz`/`time with time zone` is unsupported (None).
            "date" => Some(ColumnType::Date),
            "time" | "time without time zone" => Some(ColumnType::Time),
            "timetz" | "time with time zone" => Some(ColumnType::Timetz),
            "timestamp" | "timestamp without time zone" => Some(ColumnType::Timestamp),
            "timestamptz" | "timestamp with time zone" => Some(ColumnType::Timestamptz),
            "interval" => Some(ColumnType::Interval),
            // SP40: `bytea` — variable-length binary string.
            "bytea" => Some(ColumnType::Bytea),
            "uuid" => Some(ColumnType::Uuid),
            "regclass" => Some(ColumnType::Regclass),
            "regtype" => Some(ColumnType::Regtype),
            "regprocedure" => Some(ColumnType::Regprocedure),
            "regnamespace" => Some(ColumnType::Regnamespace),
            "regproc" => Some(ColumnType::Regproc),
            "regoper" => Some(ColumnType::Regoper),
            "regoperator" => Some(ColumnType::Regoperator),
            "regconfig" => Some(ColumnType::Regconfig),
            "regdictionary" => Some(ColumnType::Regdictionary),
            "regrole" => Some(ColumnType::Regrole),
            "regcollation" => Some(ColumnType::Regcollation),
            "oidvector" => Some(ColumnType::OidVector),
            "int2vector" => Some(ColumnType::Int2Vector),
            "tsvector" => Some(ColumnType::TsVector),
            "tsquery" => Some(ColumnType::TsQuery),
            "money" => Some(ColumnType::Money),
            "bit" => Some(ColumnType::Bit(None)),
            "varbit" | "bit varying" => Some(ColumnType::VarBit(None)),
            "inet" => Some(ColumnType::Inet),
            "cidr" => Some(ColumnType::Cidr),
            "macaddr" => Some(ColumnType::MacAddr),
            "macaddr8" => Some(ColumnType::MacAddr8),
            "json" => Some(ColumnType::Json),
            "jsonb" => Some(ColumnType::Jsonb),
            "jsonpath" => Some(ColumnType::JsonPath),
            // The anonymous composite type. `SELECT ROW(1,2)` has it, and it is
            // the declared parameter type of `json_populate_record(record, …)`.
            "record" => Some(ColumnType::Record(None)),
            "int4range" => ColumnType::builtin_range(oids::INT4RANGE),
            "numrange" => ColumnType::builtin_range(oids::NUMRANGE),
            "tsrange" => ColumnType::builtin_range(oids::TSRANGE),
            "tstzrange" => ColumnType::builtin_range(oids::TSTZRANGE),
            "daterange" => ColumnType::builtin_range(oids::DATERANGE),
            "int8range" => ColumnType::builtin_range(oids::INT8RANGE),
            "int4multirange" => ColumnType::builtin_multirange(oids::INT4MULTIRANGE),
            "nummultirange" => ColumnType::builtin_multirange(oids::NUMMULTIRANGE),
            "tsmultirange" => ColumnType::builtin_multirange(oids::TSMULTIRANGE),
            "tstzmultirange" => ColumnType::builtin_multirange(oids::TSTZMULTIRANGE),
            "datemultirange" => ColumnType::builtin_multirange(oids::DATEMULTIRANGE),
            "int8multirange" => ColumnType::builtin_multirange(oids::INT8MULTIRANGE),
            _ => None,
        }
    }

    /// The named composite this type is, if any.
    #[must_use]
    pub fn composite(self) -> Option<UserTypeRef> {
        match self {
            ColumnType::Record(named) => named,
            _ => None,
        }
    }

    /// The type a value of this type is actually stored and encoded as: a
    /// domain's base type (transitively), or the type itself.
    #[must_use]
    pub fn storage_type(self) -> ColumnType {
        let mut ty = self;
        // A domain over a domain is legal; `PostgreSQL` flattens to the base.
        for _ in 0..MAX_DOMAIN_DEPTH {
            match ty {
                ColumnType::Domain(domain) => ty = *domain.base,
                other => return other,
            }
        }
        ty
    }

    /// The one-dimensional array type over `elem`, or `None` when crabka has no
    /// array type for it (`varchar(n)`, `char(n)`, `regclass`, nested arrays).
    /// Callers report that as 0A000.
    pub fn array_of(elem: ColumnType) -> Option<Self> {
        ElemType::from_column_type(elem).map(ColumnType::Array)
    }

    /// The element type when this is an array type.
    pub fn array_element(self) -> Option<ElemType> {
        match self {
            ColumnType::Array(elem) => Some(elem),
            ColumnType::OidVector => Some(ElemType::Int4),
            ColumnType::Int2Vector => Some(ElemType::Int2),
            _ => None,
        }
    }

    pub fn oid(self) -> u32 {
        match self {
            ColumnType::Bool => oids::BOOL,
            ColumnType::Int2 => oids::INT2,
            ColumnType::Int8 => oids::INT8,
            ColumnType::Int4 => oids::INT4,
            ColumnType::Text => oids::TEXT,
            ColumnType::Varchar(_) => oids::VARCHAR,
            ColumnType::Char(_) => oids::BPCHAR,
            ColumnType::Float4 => oids::FLOAT4,
            ColumnType::Float8 => oids::FLOAT8,
            ColumnType::Point => oids::POINT,
            ColumnType::Path => oids::PATH,
            ColumnType::Lseg => oids::LSEG,
            ColumnType::Line => oids::LINE,
            ColumnType::Circle => oids::CIRCLE,
            ColumnType::Box => oids::BOX,
            ColumnType::Numeric(_) => oids::NUMERIC,
            ColumnType::Date => oids::DATE,
            ColumnType::Time => oids::TIME,
            ColumnType::Timetz => oids::TIMETZ,
            ColumnType::Timestamp => oids::TIMESTAMP,
            ColumnType::Timestamptz => oids::TIMESTAMPTZ,
            ColumnType::Interval => oids::INTERVAL,
            ColumnType::Bytea => oids::BYTEA,
            ColumnType::Uuid => oids::UUID,
            ColumnType::Regclass => oids::REGCLASS,
            ColumnType::Regtype => oids::REGTYPE,
            ColumnType::Regprocedure => oids::REGPROCEDURE,
            ColumnType::Regnamespace => oids::REGNAMESPACE,
            ColumnType::Regproc => oids::REGPROC,
            ColumnType::Regoper => oids::REGOPER,
            ColumnType::Regoperator => oids::REGOPERATOR,
            ColumnType::Regconfig => oids::REGCONFIG,
            ColumnType::Regdictionary => oids::REGDICTIONARY,
            ColumnType::Regrole => oids::REGROLE,
            ColumnType::Regcollation => oids::REGCOLLATION,
            ColumnType::OidVector => oids::OIDVECTOR,
            ColumnType::Int2Vector => oids::INT2VECTOR,
            ColumnType::TsVector => oids::TSVECTOR,
            ColumnType::TsQuery => oids::TSQUERY,
            ColumnType::Inet => oids::INET,
            ColumnType::Cidr => oids::CIDR,
            ColumnType::MacAddr => oids::MACADDR,
            ColumnType::MacAddr8 => oids::MACADDR8,
            ColumnType::Oid => oids::OID,
            ColumnType::Xid => oids::XID,
            ColumnType::Xid8 => oids::XID8,
            ColumnType::Cid => oids::CID,
            ColumnType::Tid => oids::TID,
            ColumnType::PgLsn => oids::PG_LSN,
            ColumnType::Money => oids::MONEY,
            ColumnType::Bit(_) => oids::BIT,
            ColumnType::VarBit(_) => oids::VARBIT,
            ColumnType::Json => oids::JSON,
            ColumnType::Jsonb => oids::JSONB,
            ColumnType::JsonPath => oids::JSONPATH,
            ColumnType::Array(elem) => elem.array_oid(),
            ColumnType::Record(None) => oids::RECORD,
            ColumnType::Record(Some(named)) | ColumnType::Enum(named) => named.oid,
            ColumnType::Range(range) => range.oid,
            ColumnType::Multirange(multirange) => multirange.oid,
            ColumnType::Domain(domain) => domain.oid,
        }
    }

    /// PostgreSQL type name (for error messages and FieldDescription debugging).
    pub fn name(self) -> &'static str {
        match self {
            ColumnType::Bool => "boolean",
            ColumnType::Int2 => "smallint",
            ColumnType::Int8 => "bigint",
            ColumnType::Int4 => "integer",
            ColumnType::Text => "text",
            ColumnType::Varchar(_) => "character varying",
            ColumnType::Char(_) => "character",
            ColumnType::Float4 => "real",
            ColumnType::Float8 => "double precision",
            ColumnType::Point => "point",
            ColumnType::Path => "path",
            ColumnType::Lseg => "lseg",
            ColumnType::Line => "line",
            ColumnType::Circle => "circle",
            ColumnType::Box => "box",
            ColumnType::Numeric(_) => "numeric",
            ColumnType::Date => "date",
            ColumnType::Time => "time without time zone",
            ColumnType::Timetz => "time with time zone",
            ColumnType::Timestamp => "timestamp without time zone",
            ColumnType::Timestamptz => "timestamp with time zone",
            ColumnType::Interval => "interval",
            ColumnType::Bytea => "bytea",
            ColumnType::Uuid => "uuid",
            ColumnType::Regclass => "regclass",
            ColumnType::Regtype => "regtype",
            ColumnType::Regprocedure => "regprocedure",
            ColumnType::Regnamespace => "regnamespace",
            ColumnType::Regproc => "regproc",
            ColumnType::Regoper => "regoper",
            ColumnType::Regoperator => "regoperator",
            ColumnType::Regconfig => "regconfig",
            ColumnType::Regdictionary => "regdictionary",
            ColumnType::Regrole => "regrole",
            ColumnType::Regcollation => "regcollation",
            ColumnType::OidVector => "oidvector",
            ColumnType::Int2Vector => "int2vector",
            ColumnType::TsVector => "tsvector",
            ColumnType::TsQuery => "tsquery",
            ColumnType::Inet => "inet",
            ColumnType::Cidr => "cidr",
            ColumnType::MacAddr => "macaddr",
            ColumnType::MacAddr8 => "macaddr8",
            ColumnType::Oid => "oid",
            ColumnType::Xid => "xid",
            ColumnType::Xid8 => "xid8",
            ColumnType::Cid => "cid",
            ColumnType::Tid => "tid",
            ColumnType::PgLsn => "pg_lsn",
            ColumnType::Money => "money",
            ColumnType::Bit(_) => "bit",
            ColumnType::VarBit(_) => "bit varying",
            ColumnType::Json => "json",
            ColumnType::Jsonb => "jsonb",
            ColumnType::JsonPath => "jsonpath",
            ColumnType::Array(elem) => elem.array_name(),
            ColumnType::Record(None) => "record",
            ColumnType::Record(Some(named)) | ColumnType::Enum(named) => named.name,
            ColumnType::Range(range) => range.name,
            ColumnType::Multirange(multirange) => multirange.name,
            ColumnType::Domain(domain) => domain.name,
        }
    }

    /// pg_type.typlen: fixed sizes, -1 for variable-length text/numeric.
    pub fn type_size(self) -> i16 {
        match self {
            ColumnType::Bool => 1,
            ColumnType::Int2 => 2,
            ColumnType::Int8 => 8,
            ColumnType::Int4 => 4,
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_) => -1,
            ColumnType::Float4 => 4,
            ColumnType::Float8 => 8,
            ColumnType::Point => 16,
            ColumnType::Path => -1,
            ColumnType::Lseg => 32,
            ColumnType::Line => 24,
            ColumnType::Circle => 24,
            ColumnType::Box => 32,
            ColumnType::Numeric(_) => -1,
            ColumnType::Date => 4,
            ColumnType::Time => 8,
            ColumnType::Timetz => 12,
            ColumnType::Timestamp => 8,
            ColumnType::Timestamptz => 8,
            ColumnType::Interval => 16,
            ColumnType::Bytea => -1,
            ColumnType::Uuid => 16,
            ColumnType::Regclass => 4,
            ColumnType::Regtype => 4,
            ColumnType::Regprocedure | ColumnType::Regnamespace => 4,
            // The seven remaining `reg*` types are oids too: `pg_type.typlen`
            // is 4 for every member of the family.
            ColumnType::Regproc
            | ColumnType::Regoper
            | ColumnType::Regoperator
            | ColumnType::Regconfig
            | ColumnType::Regdictionary
            | ColumnType::Regrole
            | ColumnType::Regcollation => 4,
            ColumnType::OidVector | ColumnType::Int2Vector => -1,
            ColumnType::TsVector | ColumnType::TsQuery => -1,
            // `inet`/`cidr` are varlena; the two MAC types are fixed-width.
            ColumnType::Inet | ColumnType::Cidr => -1,
            ColumnType::MacAddr => 6,
            ColumnType::MacAddr8 => 8,
            // `pg_type.typlen`: the three 32-bit identifiers are 4, `tid` is a
            // 4-byte block plus a 2-byte offset, and `xid8`/`pg_lsn` are 8.
            ColumnType::Oid | ColumnType::Xid | ColumnType::Cid => 4,
            ColumnType::Tid => 6,
            ColumnType::Xid8 | ColumnType::PgLsn => 8,
            // `money` is a pass-by-value int64; the two bit types are varlena.
            ColumnType::Money => 8,
            ColumnType::Bit(_) | ColumnType::VarBit(_) => -1,
            // json, jsonb, jsonpath, arrays and composites are variable-length.
            ColumnType::Json
            | ColumnType::Jsonb
            | ColumnType::JsonPath
            | ColumnType::Array(_)
            | ColumnType::Record(_) => -1,
            // `pg_type.typlen` of an enum is 4 (the oid of its pg_enum row).
            ColumnType::Enum(_) => 4,
            ColumnType::Range(_) | ColumnType::Multirange(_) => -1,
            // A domain has its base type's storage.
            ColumnType::Domain(domain) => domain.base.type_size(),
        }
    }

    /// True for any `numeric`, whatever its modifier. This is the common "is
    /// this the numeric type?" test that the promotion/cast logic uses.
    pub fn is_numeric(self) -> bool {
        matches!(self, ColumnType::Numeric(_))
    }

    pub fn is_string(self) -> bool {
        matches!(
            self,
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_)
        )
    }

    /// True for the eleven object-identifier types. Every one of them is an
    /// `oid` underneath — same storage, same comparison, same binary wire form
    /// — and differs only in the catalog its input and output functions read.
    /// The cast table keys off this rather than listing all eleven at each of
    /// its half-dozen sites.
    pub const fn is_reg(self) -> bool {
        matches!(
            self,
            ColumnType::Regclass
                | ColumnType::Regtype
                | ColumnType::Regprocedure
                | ColumnType::Regnamespace
                | ColumnType::Regproc
                | ColumnType::Regoper
                | ColumnType::Regoperator
                | ColumnType::Regconfig
                | ColumnType::Regdictionary
                | ColumnType::Regrole
                | ColumnType::Regcollation
        )
    }

    pub fn typmod(self) -> i32 {
        match self {
            ColumnType::Varchar(Some(n)) | ColumnType::Char(Some(n)) => i32::from(n) + 4,
            // `bittypmodin` stores the bit count with no varlena adjustment,
            // so `bit(4)`'s typmod is 4, not 8.
            ColumnType::Bit(Some(n)) | ColumnType::VarBit(Some(n)) => n,
            // A domain inherits its base type's length modifier.
            ColumnType::Domain(domain) => domain.base.typmod(),
            _ => -1,
        }
    }
}

/// How many times a domain may be defined over another domain before the
/// engine stops unwrapping. `PostgreSQL` has no fixed limit but does refuse
/// cycles; this bound makes [`ColumnType::storage_type`] total.
pub const MAX_DOMAIN_DEPTH: usize = 32;

/// A runtime value.
///
/// `PartialEq`/`Eq`/`Hash` are **hand-written** (SP30), not derived, because of
/// the `Float8` variant: a raw `f64` is not `Eq`/`Hash` (`NaN != NaN`; `-0.0`
/// and `+0.0` have distinct bit patterns yet compare equal). This crate
/// implements PostgreSQL's *grouping* equality instead (the `float8` btree
/// equality `GROUP BY`/`DISTINCT` use): all `NaN`s are one value, and
/// `-0.0 == +0.0`. The four non-float variants behave exactly as the derive did.
/// This keys `GROUP BY` group maps and aggregate `DISTINCT` sets.
///
/// SP37 adds five date/time variants that use `jiff` types. Task 3 adds their
/// `PartialEq`/`Hash` arms (grouping equality). For now they use the
/// `_ => false` catch-all in `PartialEq` and real `Hash` arms (required because
/// `Hash` is exhaustive).
#[derive(Debug, Clone)]
pub enum Datum {
    Null,
    Bool(bool),
    /// PostgreSQL `smallint`.
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Text(String),
    /// PostgreSQL `jsonpath`, stored as its canonical text representation.
    JsonPath(String),
    /// PostgreSQL `real` — single-precision float. Grouping equality and hashing
    /// follow the same rules as [`Datum::Float8`] (one NaN, `-0.0 == +0.0`).
    Float4(f32),
    /// SP30: PostgreSQL `double precision`.
    Float8(f64),
    /// PostgreSQL geometric point.
    Point(crate::geometry::Point),
    /// PostgreSQL geometric path.
    Path(crate::geometry::Path),
    /// PostgreSQL's `lseg`, a line segment between two endpoints.
    Lseg(crate::geometry::Lseg),
    /// PostgreSQL's `line`, an infinite line's three coefficients.
    Line(crate::geometry::Line),
    /// PostgreSQL's `circle`, a centre point and a radius.
    Circle(crate::geometry::Circle),
    /// PostgreSQL's `box`, an axis-aligned rectangle's two corners.
    Box(crate::geometry::Box2),
    /// SP32: PostgreSQL `numeric` — an arbitrary-precision exact decimal, or one
    /// of the `NaN` / `±Infinity` specials.
    Numeric(NumericValue),
    /// SP37: PostgreSQL `date`: a calendar date (no time-of-day, no timezone).
    Date(jiff::civil::Date),
    /// SP37: PostgreSQL `time without time zone`: time-of-day only.
    Time(jiff::civil::Time),
    /// PostgreSQL `time with time zone`: a clock reading and its UTC offset.
    Timetz(crate::datetime::TimeTz),
    /// SP37: PostgreSQL `timestamp without time zone`: date + time-of-day, no timezone.
    Timestamp(jiff::civil::DateTime),
    /// SP37: PostgreSQL `timestamp with time zone`: an instant in UTC.
    Timestamptz(jiff::Timestamp),
    /// SP37: PostgreSQL `interval`: months + days + microseconds.
    Interval(crate::datetime::Interval),
    /// SP40: PostgreSQL `bytea`: variable-length binary string (raw bytes).
    Bytea(Vec<u8>),
    /// PostgreSQL `json` — the original input text, validated by `json_in` and
    /// then left exactly as written. Holding the text rather than a parse tree
    /// is what makes `'{"b":1,   "a":2}'::json` print back unchanged.
    Json(String),
    /// PostgreSQL `jsonb` — a decomposed JSON value in canonical form.
    Jsonb(crate::jsonb::JsonbValue),
    /// A one-dimensional PostgreSQL array.
    Array(ArrayValue),
    /// PostgreSQL's zero-based oid array used by catalog signatures.
    OidVector(ArrayValue),
    /// A composite value — the anonymous `record` a `ROW(…)` produces, or a row
    /// of a type created by `CREATE TYPE … AS (…)`.
    Record(RecordValue),
    /// A value of a `CREATE TYPE … AS ENUM` type.
    Enum(EnumValue),
    /// A built-in or user-defined range with typed bounds.
    Range(RangeValue),
    /// A built-in multirange in canonical component order.
    Multirange(MultirangeValue),
    /// PostgreSQL `regclass` — a relation's `pg_class` oid plus the name
    /// `regclassout` prints for it.
    Regclass(RegclassValue),
    /// PostgreSQL full-text document/query values.
    TsVector(crate::text_search::TsVector),
    TsQuery(crate::text_search::TsQuery),
    /// PostgreSQL `money`, stored the way PostgreSQL stores it: a count of
    /// minor currency units, so `$1.00` is 100.
    Money(i64),
    /// PostgreSQL `bit` or `bit varying`. One variant for both, because the two
    /// are binary-coercible in either direction and share every operation; the
    /// value's own `varying` flag is what reports which type it is.
    BitString(crate::bitstring::BitString),
    /// PostgreSQL `inet` or `cidr`. One variant for both, because PostgreSQL
    /// stores both in one struct and compares them with one function; the
    /// value's `is_cidr` flag decides only how it renders and which SQL type
    /// it reports.
    Inet(crate::network::Inet),
    /// PostgreSQL `macaddr` — six bytes.
    MacAddr(crate::network::MacAddr),
    /// PostgreSQL `macaddr8` — eight bytes.
    MacAddr8(crate::network::MacAddr8),
    /// PostgreSQL `oid`, held **unsigned** — the whole point of the type.
    Oid(u32),
    /// PostgreSQL `xid`, a 32-bit transaction id.
    Xid(u32),
    /// PostgreSQL `xid8`, a 64-bit transaction id.
    Xid8(u64),
    /// PostgreSQL `cid`, a 32-bit command id.
    Cid(u32),
    /// PostgreSQL `tid`, a block number and an offset within it.
    Tid(crate::sysid::Tid),
    /// PostgreSQL `pg_lsn`, a 64-bit log position.
    PgLsn(u64),
}

/// A PostgreSQL range value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RangeValue {
    pub ty: RangeRef,
    pub lower: Option<Box<Datum>>,
    pub upper: Option<Box<Datum>>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub empty: bool,
}

impl RangeValue {
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        ColumnType::Range(self.ty)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultirangeValue {
    pub ty: MultirangeRef,
    pub ranges: Vec<RangeValue>,
}

impl MultirangeValue {
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        ColumnType::Multirange(self.ty)
    }
}

/// A `regclass` value: the relation oid, and the relation name that oid
/// resolves to.
///
/// `PostgreSQL` stores `regclass` as a bare oid and only consults the catalog in
/// `regclassout`. crabka cannot do that, because the wire encoder and the
/// `→ text` cast both live in this crate, which has no catalog handle. The
/// executor's cast therefore resolves the name once, where the catalog *is* in
/// scope, and the name travels with the value. The oid stays the identity:
/// comparison, hashing and the binary wire form all use it, so
/// `confrelid = 'pp'::regclass` is still an integer comparison.
#[derive(Debug, Clone)]
pub struct RegclassValue {
    /// The relation's `pg_class` oid.
    pub oid: i32,
    /// What `regclassout` prints: the relation name, double-quoted when it is
    /// not a bare lowercase identifier.
    pub name: std::sync::Arc<str>,
}

impl RegclassValue {
    /// A `regclass` whose oid no relation matches.
    ///
    /// `PostgreSQL`'s `regclassout` does not error here: it prints `-` for
    /// `InvalidOid` and the bare oid otherwise, so `SELECT 999999::oid::regclass`
    /// yields `999999`.
    ///
    /// The fallback is `oidout`, which is **unsigned**: an oid past 2^31 is
    /// stored as a negative `i32` here and still prints as
    /// `4294967295::regclass` does in PostgreSQL, not as `-1`.
    #[must_use]
    pub fn unresolved(oid: i32) -> Self {
        let name = if oid == 0 {
            "-".into()
        } else {
            (oid as u32).to_string().into()
        };
        RegclassValue { oid, name }
    }

    /// A `regclass` for a relation the catalog resolved. `name` must already be
    /// quoted as `quote_ident` would.
    #[must_use]
    pub fn resolved(oid: i32, name: &str) -> Self {
        RegclassValue {
            oid,
            name: name.into(),
        }
    }
}

/// A composite (row) value.
///
/// The field names travel with the value because the functions that consume a
/// record need them: `row_to_json`, `to_jsonb`, and `record_out` for a named
/// composite. The record may also be several joins away from the relation that
/// named its columns. The names are shared rather than cloned per row, so one
/// relation scan that produces a record per row shares a single name vector.
#[derive(Debug, Clone)]
pub struct RecordValue {
    /// The named composite type, or `None` for the anonymous `record` that a
    /// bare `ROW(…)` has.
    pub ty: Option<UserTypeRef>,
    /// The field names, positionally aligned with `values`. `PostgreSQL` names
    /// an anonymous record's fields `f1`…`fn`.
    pub names: std::sync::Arc<[String]>,
    /// The field values.
    pub values: Vec<Datum>,
}

impl RecordValue {
    /// An anonymous `record` whose fields are named `f1`…`fn`, the names
    /// `PostgreSQL` gives a bare `ROW(…)`.
    #[must_use]
    pub fn anonymous(values: Vec<Datum>) -> Self {
        let names = (1..=values.len()).map(|i| format!("f{i}")).collect();
        RecordValue {
            ty: None,
            names,
            values,
        }
    }

    /// A record with explicit field names.
    #[must_use]
    pub fn named(
        ty: Option<UserTypeRef>,
        names: std::sync::Arc<[String]>,
        values: Vec<Datum>,
    ) -> Self {
        RecordValue { ty, names, values }
    }

    /// This value's column type.
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        ColumnType::Record(self.ty)
    }

    /// The value of the field called `name`, matched exactly (the lexer has
    /// already case-folded an unquoted reference).
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&Datum> {
        let index = self.names.iter().position(|field| field == name)?;
        self.values.get(index)
    }
}

/// `PostgreSQL`'s `record_eq`: positional over the field values. Field *names*
/// are not part of the value, so `ROW(1,2) = ROW(1,2)` holds whatever the two
/// rows' columns were called. The composite type is not part of the value
/// either, so a `t_rec` row equals the anonymous row with the same fields,
/// exactly as `PostgreSQL`'s record comparison does after its implicit
/// coercion.
impl PartialEq for RecordValue {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for RecordValue {}

impl std::hash::Hash for RecordValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.values.hash(state);
    }
}

/// A value of a `CREATE TYPE … AS ENUM` type: the type it belongs to and the
/// label. This crate does not store the *ordering*. The ordering is the label's
/// position in the type's current label list, which is what `PostgreSQL` reads
/// out of
/// `pg_enum.enumsortorder` at comparison time, so `ALTER TYPE … ADD VALUE
/// BEFORE` re-orders existing values just as it does there.
#[derive(Debug, Clone, Eq)]
pub struct EnumValue {
    /// The enum type.
    pub ty: UserTypeRef,
    /// The label, exactly as declared.
    pub label: String,
}

impl EnumValue {
    /// This value's column type.
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        ColumnType::Enum(self.ty)
    }

    /// The label's position in its type's declared order, or `None` when the
    /// label is no longer part of the type (the type was dropped or the label
    /// renamed out from under a value already in flight).
    #[must_use]
    pub fn sort_order(&self) -> Option<usize> {
        crate::usertype::lookup_oid(self.ty.oid)?
            .labels()?
            .iter()
            .position(|label| *label == self.label)
    }
}

impl PartialEq for EnumValue {
    fn eq(&self, other: &Self) -> bool {
        self.ty == other.ty && self.label == other.label
    }
}

impl std::hash::Hash for EnumValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ty.hash(state);
        self.label.hash(state);
    }
}

/// The extent of one array dimension: its lower subscript bound and its length.
///
/// `PostgreSQL` stores these as the parallel `lbound[]`/`dims[]` header arrays.
/// The upper bound is `lower + len - 1`, and a dimension is never negative in
/// length. `'[1:0]={}'` is rejected at input, not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArrayDim {
    /// The subscript of this dimension's first slot.
    pub lower: i32,
    /// How many slots this dimension has.
    pub len: i32,
}

impl ArrayDim {
    /// A dimension of `len` slots starting at `lower`.
    pub fn new(lower: i32, len: i32) -> Self {
        ArrayDim { lower, len }
    }

    /// A dimension of `len` slots at `PostgreSQL`'s default lower bound of 1.
    pub fn from_len(len: usize) -> Self {
        ArrayDim {
            lower: 1,
            len: i32::try_from(len).unwrap_or(i32::MAX),
        }
    }

    /// The subscript of this dimension's last slot (`lower + len - 1`).
    pub fn upper(self) -> i32 {
        self.lower.saturating_add(self.len).saturating_sub(1)
    }
}

/// The maximum number of array dimensions, `PostgreSQL`'s `MAXDIM`.
pub const MAX_ARRAY_DIM: usize = 6;

/// An array value: a flat row-major element vector plus its dimension header.
///
/// The element type travels alongside the elements, so an empty array is still
/// typed (`'{}'::int[]` knows it is `integer[]`) and so the binary wire
/// encoding, which must emit the element OID, is context-free. `dims` is empty
/// for the zero-dimensional empty array, the only array `PostgreSQL` renders as
/// `{}` and the only one whose `array_ndims` is NULL.
#[derive(Debug, Clone)]
pub struct ArrayValue {
    /// The array's element type.
    pub elem: ElemType,
    /// The elements in row-major order; `Datum::Null` is a NULL element.
    pub elems: Vec<Datum>,
    /// One entry per dimension, outermost first; empty for an empty array.
    pub dims: Vec<ArrayDim>,
}

impl ArrayValue {
    /// A one-dimensional array of `elems` with `PostgreSQL`'s default lower
    /// bound of 1. Empty input yields the zero-dimensional empty array.
    pub fn new(elem: ElemType, elems: Vec<Datum>) -> Self {
        let dims = if elems.is_empty() {
            Vec::new()
        } else {
            vec![ArrayDim::from_len(elems.len())]
        };
        ArrayValue { elem, elems, dims }
    }

    /// An array with an explicit dimension header.
    ///
    /// The caller must make `dims` match `elems`. [`ArrayValue::new`] covers
    /// the one-dimensional case, and the array input/slice code builds the
    /// header itself. This constructor normalizes a header whose lengths do not
    /// multiply out to `elems.len()` back to one dimension, rather than leave it
    /// inconsistent.
    ///
    /// A header with **no elements** collapses to zero dimensions in the same
    /// way. `PostgreSQL` has no zero-length dimension, because `array_recv`,
    /// `construct_md_array` and the slice code all funnel an empty result into
    /// `construct_empty_array`. So `'{{},{}}'`, an out-of-order slice like
    /// `(ARRAY[1, 2, 3])[3:1]`, and the `ndim = 1, len = 0` header every libpq
    /// driver sends for an empty array are one and the same value, the one whose
    /// `array_ndims` is NULL.
    pub fn with_dims(elem: ElemType, elems: Vec<Datum>, dims: Vec<ArrayDim>) -> Self {
        let product: usize = dims
            .iter()
            .map(|d| usize::try_from(d.len).unwrap_or(0))
            .product();
        if dims.is_empty() || elems.is_empty() || product != elems.len() {
            return ArrayValue::new(elem, elems);
        }
        ArrayValue { elem, elems, dims }
    }

    /// The array's column type.
    pub fn column_type(&self) -> ColumnType {
        ColumnType::Array(self.elem)
    }

    /// How many dimensions the array has (0 for the empty array).
    pub fn ndims(&self) -> usize {
        self.dims.len()
    }

    /// Does any dimension start somewhere other than 1? This controls whether
    /// `array_out` emits the `[l:u]=` header.
    pub fn has_explicit_bounds(&self) -> bool {
        self.dims.iter().any(|d| d.lower != 1)
    }

    /// The strides of each dimension in the flat element vector, outermost first.
    pub fn strides(&self) -> Vec<usize> {
        let mut strides = vec![1usize; self.dims.len()];
        for i in (0..self.dims.len().saturating_sub(1)).rev() {
            let len = usize::try_from(self.dims[i + 1].len).unwrap_or(0);
            strides[i] = strides[i + 1].saturating_mul(len);
        }
        strides
    }
}

impl PartialEq for ArrayValue {
    /// `PostgreSQL`'s `array_eq`: the dimension header, lengths **and** lower
    /// bounds, must match before this method compares the elements, so
    /// `'[2:4]={1,2,3}' = '{1,2,3}'` is false.
    fn eq(&self, other: &Self) -> bool {
        self.elem == other.elem && self.dims == other.dims && self.elems == other.elems
    }
}

impl Eq for ArrayValue {}

impl std::hash::Hash for ArrayValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.elem.hash(state);
        self.dims.hash(state);
        self.elems.hash(state);
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Bool(a), Datum::Bool(b)) => a == b,
            (Datum::Int2(a), Datum::Int2(b)) => a == b,
            (Datum::Int4(a), Datum::Int4(b)) => a == b,
            (Datum::Int8(a), Datum::Int8(b)) => a == b,
            (Datum::Text(a), Datum::Text(b)) => a == b,
            (Datum::JsonPath(a), Datum::JsonPath(b)) => a == b,
            // Grouping equality: `NaN == NaN` (Rust's `==` says false, hence the
            // explicit NaN arm) and `-0.0 == +0.0` (Rust's `==` already says true).
            (Datum::Float4(a), Datum::Float4(b)) => a == b || (a.is_nan() && b.is_nan()),
            (Datum::Float8(a), Datum::Float8(b)) => a == b || (a.is_nan() && b.is_nan()),
            (Datum::Point(a), Datum::Point(b)) => a == b,
            (Datum::Path(a), Datum::Path(b)) => a == b,
            (Datum::Lseg(a), Datum::Lseg(b)) => a == b,
            (Datum::Line(a), Datum::Line(b)) => a == b,
            (Datum::Circle(a), Datum::Circle(b)) => a == b,
            (Datum::Box(a), Datum::Box(b)) => a == b,
            // SP32: numeric grouping equality is by VALUE, ignoring scale, so
            // `1.0` and `1.00` group together (`bigdecimal`'s `==` already does
            // this), and — as in PostgreSQL's `numeric_eq` — `NaN` equals `NaN`.
            (Datum::Numeric(a), Datum::Numeric(b)) => a == b,
            // SP37: jiff civil types implement PartialEq by calendar/clock value.
            (Datum::Date(a), Datum::Date(b)) => a == b,
            (Datum::Time(a), Datum::Time(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            // timestamptz equality is by absolute instant (jiff Timestamp).
            (Datum::Timestamptz(a), Datum::Timestamptz(b)) => a == b,
            (Datum::Timetz(a), Datum::Timetz(b)) => a == b,
            // interval uses its canonical-estimate Eq (Task 2).
            (Datum::Interval(a), Datum::Interval(b)) => a == b,
            // SP40: bytea equality is byte-for-byte (matches PostgreSQL's `byteaeq`).
            (Datum::Bytea(a), Datum::Bytea(b)) => a == b,
            // jsonb equality is structural over the canonical form (key order is
            // already normalized; number scale is ignored, as in `numeric`).
            // `PostgreSQL` declares no equality operator for `json`, so nothing
            // in SQL can reach this arm through `=`. It exists because `Datum`
            // is `PartialEq` for the executor's own bookkeeping (unchanged-row
            // detection, default comparison), where two `json` values are the
            // same only when their text is.
            (Datum::Json(a), Datum::Json(b)) => a == b,
            (Datum::Jsonb(a), Datum::Jsonb(b)) => a == b,
            // Arrays are equal when their element type and every element are.
            (Datum::Array(a), Datum::Array(b)) => a == b,
            (Datum::OidVector(a), Datum::OidVector(b)) => a == b,
            // Composites compare field by field; enums by type and label.
            (Datum::Record(a), Datum::Record(b)) => a == b,
            (Datum::Enum(a), Datum::Enum(b)) => a == b,
            // The oid is the `regclass` identity; the name is derived from it.
            (Datum::Regclass(a), Datum::Regclass(b)) => a.oid == b.oid,
            (Datum::TsVector(a), Datum::TsVector(b)) => a == b,
            (Datum::TsQuery(a), Datum::TsQuery(b)) => a == b,
            // `Inet`'s own `PartialEq` ignores `is_cidr`, so a `cidr` and an
            // `inet` naming the same address are one value — which is what
            // PostgreSQL's shared `network_cmp` gives `'x'::cidr = 'x'::inet`.
            (Datum::Money(a), Datum::Money(b)) => a == b,
            // `BitString`'s own `PartialEq` ignores `varying`, so a `bit` and a
            // `bit varying` holding the same bits are one value.
            (Datum::BitString(a), Datum::BitString(b)) => a == b,
            (Datum::Inet(a), Datum::Inet(b)) => a == b,
            (Datum::MacAddr(a), Datum::MacAddr(b)) => a == b,
            (Datum::MacAddr8(a), Datum::MacAddr8(b)) => a == b,
            (Datum::Oid(a), Datum::Oid(b))
            | (Datum::Xid(a), Datum::Xid(b))
            | (Datum::Cid(a), Datum::Cid(b)) => a == b,
            (Datum::Xid8(a), Datum::Xid8(b)) | (Datum::PgLsn(a), Datum::PgLsn(b)) => a == b,
            (Datum::Tid(a), Datum::Tid(b)) => a == b,
            (Datum::Range(a), Datum::Range(b)) => a == b,
            (Datum::Multirange(a), Datum::Multirange(b)) => a == b,
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
            Datum::Int2(n) => n.hash(state),
            Datum::Int4(n) => n.hash(state),
            Datum::Int8(n) => n.hash(state),
            Datum::Text(s) => s.hash(state),
            Datum::JsonPath(s) => s.hash(state),
            // Canonicalized exactly like `Float8` below, one width down.
            Datum::Float4(f) => {
                let bits = if f.is_nan() {
                    0x7fc0_0000u32 // canonical quiet NaN
                } else if *f == 0.0 {
                    0u32 // both -0.0 and +0.0 map here
                } else {
                    f.to_bits()
                };
                bits.hash(state);
            }
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
            Datum::Point(point) => point.hash(state),
            Datum::Path(path) => path.hash(state),
            Datum::Lseg(lseg) => lseg.hash(state),
            Datum::Line(line) => line.hash(state),
            Datum::Circle(circle) => circle.hash(state),
            Datum::Box(value) => value.hash(state),
            // SP32: `NumericValue` hashes the scale-normalized form so values
            // that compare equal (`1.0` and `1.00`) hash equally.
            Datum::Numeric(d) => d.hash(state),
            // SP37: jiff types all implement Hash by value. `Interval` hashes its
            // `canonical_micros` — the same quantity its `PartialEq` compares — so
            // `1 mon` and `30 days` hash alike (the Hash/Eq contract).
            Datum::Date(d) => d.hash(state),
            Datum::Time(t) => t.hash(state),
            Datum::Timestamp(dt) => dt.hash(state),
            Datum::Timestamptz(ts) => ts.hash(state),
            Datum::Timetz(t) => t.hash(state),
            Datum::Interval(i) => i.hash(state),
            // SP40: bytea hashes its bytes.
            Datum::Bytea(b) => b.hash(state),
            // Both hash scale-normalized numbers internally, matching `Eq`.
            Datum::Json(text) => text.hash(state),
            Datum::Jsonb(j) => j.hash(state),
            Datum::Array(a) => a.hash(state),
            Datum::OidVector(a) => a.hash(state),
            Datum::Record(r) => r.hash(state),
            Datum::Enum(e) => e.hash(state),
            // Hashes the oid alone, matching the `PartialEq` arm above.
            Datum::Regclass(r) => r.oid.hash(state),
            Datum::TsVector(v) => v.hash(state),
            Datum::TsQuery(q) => q.hash(state),
            Datum::Money(value) => value.hash(state),
            Datum::BitString(value) => value.hash(state),
            Datum::Inet(value) => value.hash(state),
            Datum::MacAddr(value) => value.hash(state),
            Datum::MacAddr8(value) => value.hash(state),
            Datum::Oid(value) | Datum::Xid(value) | Datum::Cid(value) => value.hash(state),
            Datum::Xid8(value) | Datum::PgLsn(value) => value.hash(state),
            Datum::Tid(value) => value.hash(state),
            Datum::Range(range) => range.hash(state),
            Datum::Multirange(multirange) => multirange.hash(state),
        }
    }
}

impl Datum {
    /// The non-null column type of this value (None for NULL).
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(ColumnType::Bool),
            Datum::Int2(_) => Some(ColumnType::Int2),
            Datum::Int4(_) => Some(ColumnType::Int4),
            Datum::Int8(_) => Some(ColumnType::Int8),
            Datum::Text(_) => Some(ColumnType::Text),
            Datum::JsonPath(_) => Some(ColumnType::JsonPath),
            Datum::Float4(_) => Some(ColumnType::Float4),
            Datum::Float8(_) => Some(ColumnType::Float8),
            Datum::Point(_) => Some(ColumnType::Point),
            Datum::Path(_) => Some(ColumnType::Path),
            Datum::Lseg(_) => Some(ColumnType::Lseg),
            Datum::Line(_) => Some(ColumnType::Line),
            Datum::Circle(_) => Some(ColumnType::Circle),
            Datum::Box(_) => Some(ColumnType::Box),
            // The runtime value carries no typmod — it is unconstrained `numeric`.
            Datum::Numeric(_) => Some(ColumnType::Numeric(None)),
            Datum::Date(_) => Some(ColumnType::Date),
            Datum::Time(_) => Some(ColumnType::Time),
            Datum::Timestamp(_) => Some(ColumnType::Timestamp),
            Datum::Timestamptz(_) => Some(ColumnType::Timestamptz),
            Datum::Timetz(_) => Some(ColumnType::Timetz),
            Datum::Interval(_) => Some(ColumnType::Interval),
            Datum::Bytea(_) => Some(ColumnType::Bytea),
            Datum::Json(_) => Some(ColumnType::Json),
            Datum::Jsonb(_) => Some(ColumnType::Jsonb),
            Datum::Array(a) => Some(a.column_type()),
            Datum::OidVector(_) => Some(ColumnType::OidVector),
            Datum::Record(r) => Some(r.column_type()),
            Datum::Enum(e) => Some(e.column_type()),
            Datum::Regclass(_) => Some(ColumnType::Regclass),
            Datum::TsVector(_) => Some(ColumnType::TsVector),
            Datum::TsQuery(_) => Some(ColumnType::TsQuery),
            Datum::Money(_) => Some(ColumnType::Money),
            // A runtime bit string carries no typmod; what it does carry is
            // which of the two SQL types produced it.
            Datum::BitString(value) => Some(if value.varying {
                ColumnType::VarBit(None)
            } else {
                ColumnType::Bit(None)
            }),
            Datum::Inet(value) => Some(if value.is_cidr {
                ColumnType::Cidr
            } else {
                ColumnType::Inet
            }),
            Datum::MacAddr(_) => Some(ColumnType::MacAddr),
            Datum::MacAddr8(_) => Some(ColumnType::MacAddr8),
            Datum::Oid(_) => Some(ColumnType::Oid),
            Datum::Xid(_) => Some(ColumnType::Xid),
            Datum::Xid8(_) => Some(ColumnType::Xid8),
            Datum::Cid(_) => Some(ColumnType::Cid),
            Datum::Tid(_) => Some(ColumnType::Tid),
            Datum::PgLsn(_) => Some(ColumnType::PgLsn),
            Datum::Range(range) => Some(range.column_type()),
            Datum::Multirange(multirange) => Some(multirange.column_type()),
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
/// elements. Object key order is not a concern, because this crate stores
/// `jsonb` in canonical order.
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
        // A special has one spelling already, so only a finite value can differ
        // by display scale.
        Datum::Numeric(NumericValue::Finite(d)) => {
            let normalized = crate::numeric::canonical(d.normalized());
            if normalized.fractional_digit_count() == d.fractional_digit_count() {
                Cow::Borrowed(value)
            } else {
                Cow::Owned(Datum::Numeric(NumericValue::Finite(normalized)))
            }
        }
        // `-0.0 == 0.0` and every NaN is one value under `Datum`'s grouping
        // equality, but their bit patterns differ.
        Datum::Float8(f) if f.is_nan() => Cow::Owned(Datum::Float8(f64::NAN)),
        Datum::Float8(f) if *f == 0.0 && f.is_sign_negative() => Cow::Owned(Datum::Float8(0.0)),
        Datum::Float4(f) if f.is_nan() => Cow::Owned(Datum::Float4(f32::NAN)),
        Datum::Float4(f) if *f == 0.0 && f.is_sign_negative() => Cow::Owned(Datum::Float4(0.0)),
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
        // A composite's fields need the same canonicalization as an array's
        // elements: an index over a composite column must not distinguish
        // `ROW(1.0)` from `ROW(1.00)` when `=` does not.
        Datum::Record(r) => {
            let mut changed = false;
            let values = r
                .values
                .iter()
                .map(|v| match canonicalize_for_key(v) {
                    Cow::Owned(v) => {
                        changed = true;
                        v
                    }
                    Cow::Borrowed(v) => v.clone(),
                })
                .collect();
            if changed {
                Cow::Owned(Datum::Record(RecordValue {
                    ty: r.ty,
                    names: std::sync::Arc::clone(&r.names),
                    values,
                }))
            } else {
                Cow::Borrowed(value)
            }
        }
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
                Cow::Owned(Datum::Array(ArrayValue::with_dims(
                    a.elem,
                    elems,
                    a.dims.clone(),
                )))
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
            ColumnType::from_sql_name("varbit"),
            Some(ColumnType::VarBit(None))
        );
        assert_eq!(
            ColumnType::from_sql_name("bit varying"),
            Some(ColumnType::VarBit(None))
        );
        assert_eq!(
            ColumnType::from_sql_name("bit"),
            Some(ColumnType::Bit(None))
        );
        assert_eq!(ColumnType::from_sql_name("money"), Some(ColumnType::Money));
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
        assert_eq!(ColumnType::from_sql_name("widget"), None);
        assert_eq!(ColumnType::from_sql_name("uuid"), Some(ColumnType::Uuid));
    }

    #[test]
    fn built_in_ranges_keep_postgres_identity_and_subtype() {
        for (name, oid, subtype) in [
            ("int4range", oids::INT4RANGE, ColumnType::Int4),
            ("numrange", oids::NUMRANGE, ColumnType::Numeric(None)),
            ("tsrange", oids::TSRANGE, ColumnType::Timestamp),
            ("tstzrange", oids::TSTZRANGE, ColumnType::Timestamptz),
            ("daterange", oids::DATERANGE, ColumnType::Date),
            ("int8range", oids::INT8RANGE, ColumnType::Int8),
        ] {
            let Some(ColumnType::Range(range)) = ColumnType::from_sql_name(name) else {
                panic!("{name} must resolve as a range");
            };
            assert_eq!(
                (range.oid, range.name, *range.subtype),
                (oid, name, subtype)
            );
        }
    }

    /// `oid` is its own type, not a spelling of `int4`. The distinction is not
    /// cosmetic: an `int4` alias cannot hold 4294967295, and this test would
    /// have passed while `'4294967295'::oid` raised `out of range for type
    /// integer`, which is how the alias survived.
    #[test]
    fn oid_is_its_own_type_not_an_int4_alias() {
        use assert2::assert;
        assert!(ColumnType::from_sql_name("oid") == Some(ColumnType::Oid));
        assert!(ColumnType::from_sql_name("OID") == Some(ColumnType::Oid));
        assert!(ColumnType::Oid.oid() == 26);
        assert!(ColumnType::Oid.name() == "oid");
        assert!(ColumnType::Oid.type_size() == 4);
        assert!(oids::OID == 26);
        // The value `int4` cannot hold, and the negative input `int4` would
        // have rejected.
        let utc = jiff::tz::TimeZone::UTC;
        let parse = |text: &str| {
            crate::cast::cast_in(
                &Datum::Text(text.to_string()),
                ColumnType::Oid,
                crate::encoding::OutputStyle::with_zone(&utc),
            )
        };
        assert!(parse("4294967295") == Ok(Datum::Oid(u32::MAX)));
        assert!(parse("-1") == Ok(Datum::Oid(u32::MAX)));
        assert!(Datum::Oid(u32::MAX).column_type() == Some(ColumnType::Oid));
        // Unsigned ordering: an `int4` would put 4294967295 below 1.
        assert!(
            crate::ops::compare(&Datum::Oid(u32::MAX), &Datum::Oid(1))
                == Ok(Some(std::cmp::Ordering::Greater))
        );
    }

    /// The other five system identifier types resolve too, at PostgreSQL's own
    /// oids and `typlen`s.
    #[test]
    fn the_system_identifier_types_resolve_by_name() {
        use assert2::assert;
        for (name, ty, oid, size) in [
            ("xid", ColumnType::Xid, 28_u32, 4_i16),
            ("xid8", ColumnType::Xid8, 5069, 8),
            ("cid", ColumnType::Cid, 29, 4),
            ("tid", ColumnType::Tid, 27, 6),
            ("pg_lsn", ColumnType::PgLsn, 3220, 8),
        ] {
            assert!(ColumnType::from_sql_name(name) == Some(ty), "{name}");
            assert!(ty.oid() == oid, "{name}");
            assert!(ty.name() == name, "{name}");
            assert!(ty.type_size() == size, "{name}");
        }
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

    /// `pg_type` metadata for the two new scalars, and the spellings the parser
    /// resolves. Values are `SELECT typname, typlen FROM pg_type WHERE oid IN
    /// (21, 700)` on PostgreSQL 18.4.
    #[test]
    fn int2_and_float4_report_postgres_oid_name_and_typlen() {
        use assert2::assert;
        let expected: &[(ColumnType, u32, &str, i16, &[&str])] = &[
            (
                ColumnType::Int2,
                21,
                "smallint",
                2,
                &["int2", "smallint", "SMALLINT"],
            ),
            (
                ColumnType::Float4,
                700,
                "real",
                4,
                &["float4", "real", "REAL"],
            ),
        ];
        for (ty, oid, name, typlen, spellings) in expected {
            assert!(ty.oid() == *oid, "{ty:?} oid");
            assert!(ty.name() == *name, "{ty:?} name");
            assert!(ty.type_size() == *typlen, "{ty:?} typlen");
            // Neither type carries a modifier, so RowDescription reports -1.
            assert!(ty.typmod() == -1, "{ty:?} typmod");
            for spelling in *spellings {
                assert!(
                    ColumnType::from_sql_name(spelling) == Some(*ty),
                    "{spelling} resolves"
                );
            }
        }
        assert!(Datum::Int2(1).column_type() == Some(ColumnType::Int2));
        assert!(Datum::Float4(1.5).column_type() == Some(ColumnType::Float4));
        assert!(ColumnType::array_of(ColumnType::Int2) == Some(ColumnType::Array(ElemType::Int2)));
        assert!(
            ColumnType::array_of(ColumnType::Float4) == Some(ColumnType::Array(ElemType::Float4))
        );
    }

    /// `float4` grouping equality is `float8`'s, one width down: every NaN is
    /// one value, `-0.0` and `+0.0` are one value, and equal values hash equally
    /// (the `Hash`/`Eq` contract the `GROUP BY` map depends on).
    #[test]
    fn float4_grouping_equality_and_hash_match_float8() {
        use assert2::assert;
        let nan = Datum::Float4(f32::NAN);
        let other_nan = Datum::Float4(f32::from_bits(0x7fc0_0001));
        assert!(nan == other_nan);
        assert!(hash_of(&nan) == hash_of(&other_nan));
        assert!(Datum::Float4(-0.0) == Datum::Float4(0.0));
        assert!(hash_of(&Datum::Float4(-0.0)) == hash_of(&Datum::Float4(0.0)));
        assert!(Datum::Float4(1.5) != Datum::Float4(2.5));
        // A NaN never equals a finite value, in either operand position.
        assert!(nan != Datum::Float4(1.0));
        assert!(Datum::Float4(1.0) != nan);
        // Distinct variants never collide, even at the same numeric value.
        assert!(Datum::Float4(1.0) != Datum::Float8(1.0));
        assert!(Datum::Int2(1) != Datum::Int4(1));
        assert!(Datum::Int2(1) == Datum::Int2(1));
        // Both float widths canonicalize to one index-key form.
        for (left, right) in [
            (Datum::Float4(-0.0), Datum::Float4(0.0)),
            (nan.clone(), other_nan),
        ] {
            let (a, b) = (canonicalize_for_key(&left), canonicalize_for_key(&right));
            assert!(*a == *b, "canonical form of {left:?}");
            assert!(
                crate::encoding::encode_text(&a, &jiff::tz::TimeZone::UTC)
                    == crate::encoding::encode_text(&b, &jiff::tz::TimeZone::UTC)
            );
        }
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
    fn datetime_type_names_resolve_including_timetz() {
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
        assert2::assert!(ColumnType::from_sql_name("timetz") == Some(ColumnType::Timetz));
        assert2::assert!(
            ColumnType::from_sql_name("time with time zone") == Some(ColumnType::Timetz)
        );
        assert2::assert!(ColumnType::Timetz.oid() == 1266);
        assert2::assert!(ColumnType::Timetz.name() == "time with time zone");
        assert2::assert!(ColumnType::Timetz.type_size() == 12);
    }

    /// SP37 mutation-killer for the `(Timestamptz, Timestamptz)` arm of
    /// `Datum`'s `PartialEq` + `Hash`. Two timestamptz Datums at the SAME
    /// absolute instant (parsed from different wall-clock/offset spellings) are
    /// EQUAL and hash-equal, and two at DIFFERENT instants are unequal. This
    /// pins the deleted-arm (#147), `== → !=` (#148), and `hash with ()` (#149)
    /// mutants. The existing `datetime_datum_grouping_equality_and_hash` covers
    /// Date/Interval but not Timestamptz distinctly.
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
        assert!(ColumnType::from_sql_name("JSON") == Some(ColumnType::Json));
        assert!(jsonb("1").column_type() == Some(ColumnType::Jsonb));
    }

    #[test]
    fn jsonpath_column_type_reports_postgres_oid_name_and_size() {
        use assert2::assert;
        assert!(ColumnType::JsonPath.oid() == oids::JSONPATH);
        assert!(ColumnType::JsonPath.name() == "jsonpath");
        assert!(ColumnType::JsonPath.type_size() == -1);
        assert!(ColumnType::JsonPath.typmod() == -1);
        assert!(ColumnType::from_sql_name("JSONPATH") == Some(ColumnType::JsonPath));
        assert!(Datum::JsonPath("$".into()).column_type() == Some(ColumnType::JsonPath));
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
            (ElemType::Json, 114, 199, "json[]"),
            (ElemType::Jsonb, 3802, 3807, "jsonb[]"),
            (ElemType::JsonPath, 4072, 4073, "jsonpath[]"),
            (ElemType::Int2, 21, 1005, "smallint[]"),
            (ElemType::Float4, 700, 1021, "real[]"),
            (ElemType::Varchar(None), 1043, 1015, "character varying[]"),
            (ElemType::Char(None), 1042, 1014, "character[]"),
            (ElemType::Regtype, 2206, 2211, "regtype[]"),
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
        assert!(ElemType::from_array_oid(oids::JSONARRAY) == Some(ElemType::Json));
        assert!(ElemType::from_array_oid(9999) == None);
    }

    /// The element codes are persisted (row encoding, catalog schema), so they
    /// are append-only: this pins every existing value.
    #[test]
    fn element_type_codes_are_stable_and_round_trip() {
        use assert2::assert;
        for elem in ElemType::ALL {
            assert!(ElemType::from_code(elem.code()) == Some(elem), "{elem:?}");
        }
        assert!(ElemType::JsonPath.code() == 21);
        assert!(ElemType::from_code(200) == None);
    }

    /// The lossless storage codec must carry the length modifier the bare
    /// `code()` byte drops, for every element type.
    #[test]
    fn element_type_storage_codes_round_trip_with_their_modifier() {
        use assert2::assert;
        let mut every: Vec<ElemType> = ElemType::ALL.to_vec();
        every.extend([
            ElemType::Varchar(Some(5)),
            ElemType::Varchar(Some(u16::MAX)),
            ElemType::Char(Some(1)),
        ]);
        for elem in every {
            let mut bytes = Vec::new();
            elem.write_code(&mut bytes);
            let mut cursor = bytes.as_slice();
            assert!(ElemType::read_code(&mut cursor) == Some(elem), "{elem:?}");
            assert!(cursor.is_empty(), "{elem:?}");
        }
        assert!(ElemType::read_code(&mut [].as_slice()) == None);
        assert!(ElemType::read_code(&mut [200u8].as_slice()) == None);
        // A truncated varchar payload is rejected rather than defaulted.
        assert!(ElemType::read_code(&mut [16u8].as_slice()) == None);
        assert!(ElemType::read_code(&mut [16u8, 1, 0].as_slice()) == None);
    }

    /// `PostgreSQL`'s `array_eq` compares the whole dimension header, lengths
    /// AND lower bounds, before it looks at any element.
    #[test]
    fn array_equality_and_hashing_include_the_dimension_header() {
        use std::collections::HashSet;

        use assert2::assert;

        let ints = |values: &[i32]| values.iter().copied().map(Datum::Int4).collect::<Vec<_>>();
        let flat = ArrayValue::new(ElemType::Int4, ints(&[1, 2, 3]));
        let shifted =
            ArrayValue::with_dims(ElemType::Int4, ints(&[1, 2, 3]), vec![ArrayDim::new(2, 3)]);
        let square = ArrayValue::with_dims(
            ElemType::Int4,
            ints(&[1, 2, 3, 4]),
            vec![ArrayDim::new(1, 2), ArrayDim::new(1, 2)],
        );
        assert!(flat != shifted);
        assert!(flat == ArrayValue::new(ElemType::Int4, ints(&[1, 2, 3])));
        assert!(square != ArrayValue::new(ElemType::Int4, ints(&[1, 2, 3, 4])));
        let mut set = HashSet::new();
        for value in [&flat, &shifted, &square] {
            set.insert(Datum::Array(value.clone()));
        }
        assert!(set.len() == 3);
        assert!(set.contains(&Datum::Array(ArrayValue::new(
            ElemType::Int4,
            ints(&[1, 2, 3])
        ))));
    }

    /// The dimension accessors, including the empty array's zero dimensions.
    #[test]
    fn array_dimension_accessors_describe_the_header() {
        use assert2::assert;
        let empty = ArrayValue::new(ElemType::Int4, Vec::new());
        assert!(empty.ndims() == 0);
        assert!(empty.dims.is_empty());
        assert!(!empty.has_explicit_bounds());

        let cube = ArrayValue::with_dims(
            ElemType::Int4,
            (1..=12).map(Datum::Int4).collect(),
            vec![
                ArrayDim::new(0, 2),
                ArrayDim::new(1, 2),
                ArrayDim::new(1, 3),
            ],
        );
        assert!(cube.ndims() == 3);
        assert!(cube.has_explicit_bounds());
        assert!(cube.strides() == vec![6, 3, 1]);
        assert!(cube.dims[0].upper() == 1);
        assert!(cube.dims[2].upper() == 3);

        // A header that does not multiply out to the element count is
        // normalized back to one dimension rather than left inconsistent.
        let wrong = ArrayValue::with_dims(
            ElemType::Int4,
            (1..=3).map(Datum::Int4).collect(),
            vec![ArrayDim::new(1, 2), ArrayDim::new(1, 2)],
        );
        assert!(wrong.dims == vec![ArrayDim::new(1, 3)]);
    }

    /// `PostgreSQL` has no zero-length dimension: an array with no elements is
    /// the zero-dimensional empty array, whatever header its producer declared.
    /// `array_recv` collapses the `ndim = 1, len = 0` form drivers send, and an
    /// out-of-order slice like `(ARRAY[1, 2, 3])[3:1]` collapses the same way.
    #[test]
    fn a_header_with_no_elements_collapses_to_zero_dimensions() {
        use assert2::assert;
        let empty = ArrayValue::new(ElemType::Int4, Vec::new());
        for header in [
            vec![ArrayDim::new(1, 0)],
            vec![ArrayDim::new(3, 0)],
            vec![ArrayDim::new(1, 2), ArrayDim::new(1, 0)],
        ] {
            let collapsed = ArrayValue::with_dims(ElemType::Int4, Vec::new(), header.clone());
            assert!(collapsed == empty, "{header:?}");
            assert!(collapsed.ndims() == 0, "{header:?}");
        }
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
        // The length-modified string types DO have array types, and the
        // modifier rides along on the element type.
        assert!(
            ColumnType::array_of(ColumnType::Varchar(Some(8)))
                == Some(ColumnType::Array(ElemType::Varchar(Some(8))))
        );
        assert!(
            ColumnType::array_of(ColumnType::Char(Some(2)))
                == Some(ColumnType::Array(ElemType::Char(Some(2))))
        );
        // `regclass` has none, and PostgreSQL has no nested array TYPE — an
        // array of an array is refused, the extra dimensions living in values.
        for unsupported in [ColumnType::Regclass, ColumnType::Array(ElemType::Int4)] {
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
        let a = Datum::Numeric(NumericValue::from(
            BigDecimal::from_str("1.0").expect("1.0"),
        ));
        let b = Datum::Numeric(NumericValue::from(
            BigDecimal::from_str("1.00").expect("1.00"),
        ));
        assert_eq!(a, b, "numeric equality is by value, ignoring scale");
        let c = Datum::Numeric(NumericValue::from(
            BigDecimal::from_str("2.0").expect("2.0"),
        ));
        assert_ne!(a, c);
    }
}
