//! Versioned (de)serialization of a table schema — the value stored under
//! `crabka_pgkv::key::catalog_key(name)`. Format: version byte (`5`), `table_id`
//! (u32 BE), column count (u32 BE), then per column: u32 name length, name bytes,
//! type tag; table option flags (u8); followed by a `foreign` flag byte: `0` =
//! ordinary table (no further payload), `1` = foreign table (server name len u32,
//! server name bytes, option count u32, then per option: key len u32, key bytes,
//! value len u32, value bytes).
//!
//! Foreign-data-wrapper, foreign-server, and user-mapping objects use their own
//! simple binary format (not the schema format).

use crabka_pgkv::KvError;
use crabka_pgtypes::{ColumnType, Datum, numeric::Typmod};

use crate::{
    Column, ColumnDefault, ForeignDataWrapper, ForeignServer, ForeignTableMeta, HashSharding,
    Index, IndexConstraint, IndexPlacement, Sequence, ShardingStrategy, TableOptions, UserMapping,
    View,
};

/// The single schema-value format version. All tables (ordinary and foreign)
/// are written with this version byte; a flag byte after the column list
/// distinguishes ordinary (`0`) from foreign (`1`).
pub const SCHEMA_VERSION: u8 = 5;

const TABLE_OPTION_SHARDED: u8 = 0b0000_0001;
const SHARDING_VERSION: u8 = 1;
const SHARDING_NONE: u8 = 0;
const SHARDING_HASH: u8 = 1;
const INDEX_VERSION: u8 = 2;
const SEQUENCE_VERSION: u8 = 1;
const INDEX_PLACEMENT_LOCAL: u8 = 0;
const INDEX_PLACEMENT_GLOBAL: u8 = 1;
const INDEX_CONSTRAINT_NONE: u8 = 0;
const INDEX_CONSTRAINT_PRIMARY_KEY: u8 = 1;
const INDEX_CONSTRAINT_UNIQUE: u8 = 2;

mod datum_tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
    pub const FLOAT8: u8 = 5;
    pub const NUMERIC: u8 = 6;
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
}

/// Append a column's type (tag byte, plus the numeric typmod payload).
pub(crate) fn write_type(out: &mut Vec<u8>, ty: ColumnType) {
    match ty {
        ColumnType::Bool => out.push(type_tag::BOOL),
        ColumnType::Int4 => out.push(type_tag::INT4),
        ColumnType::Int8 => out.push(type_tag::INT8),
        ColumnType::Text => out.push(type_tag::TEXT),
        ColumnType::Varchar(limit) => write_optional_u16_type(out, type_tag::VARCHAR, limit),
        ColumnType::Char(limit) => write_optional_u16_type(out, type_tag::BPCHAR, limit),
        ColumnType::Float8 => out.push(type_tag::FLOAT8),
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
    }
}

/// Read a column's type, consuming the tag (and the numeric typmod payload).
pub(crate) fn read_type(cur: &mut &[u8]) -> Result<ColumnType, KvError> {
    Ok(match take_u8(cur)? {
        type_tag::BOOL => ColumnType::Bool,
        type_tag::INT4 => ColumnType::Int4,
        type_tag::INT8 => ColumnType::Int8,
        type_tag::TEXT => ColumnType::Text,
        type_tag::VARCHAR => ColumnType::Varchar(read_optional_u16_type(cur)?),
        type_tag::BPCHAR => ColumnType::Char(read_optional_u16_type(cur)?),
        type_tag::FLOAT8 => ColumnType::Float8,
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
        Datum::Float8(value) => {
            out.push(datum_tag::FLOAT8);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Datum::Numeric(value) => {
            out.push(datum_tag::NUMERIC);
            write_str(out, &value.to_string());
        }
        Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_)
        | Datum::Bytea(_) => unreachable!("unsupported defaults are rejected before catalog write"),
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
        datum_tag::INT4 => Datum::Int4(i32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4"))),
        datum_tag::INT8 => Datum::Int8(i64::from_be_bytes(take_n(cur, 8)?.try_into().expect("8"))),
        datum_tag::TEXT => Datum::Text(read_string(cur)?),
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
        tag => {
            return Err(KvError::CorruptRow(format!(
                "unknown default datum tag {tag}"
            )));
        }
    };
    Ok(value)
}

// ── Options helpers ───────────────────────────────────────────────────────────

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(
        &u32::try_from(s.len())
            .expect("catalog string length must fit in u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(s.as_bytes());
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

fn read_string(cur: &mut &[u8]) -> Result<String, KvError> {
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
#[must_use]
pub fn serialize_schema(
    table_id: u32,
    columns: &[Column],
    options: TableOptions,
    meta: Option<&ForeignTableMeta>,
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
    out
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
            if column_count == 0 {
                return Err(KvError::CorruptRow(
                    "hash sharding requires at least one column".into(),
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
            )))
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
#[must_use]
pub fn serialize_index(index: &Index) -> Vec<u8> {
    let mut out = vec![INDEX_VERSION];
    out.extend_from_slice(&index.id.to_be_bytes());
    write_str(&mut out, &index.name);
    out.extend_from_slice(&index.table_id.to_be_bytes());
    write_str(&mut out, &index.table);
    out.push(u8::from(index.unique));
    out.push(match index.placement {
        IndexPlacement::Local => INDEX_PLACEMENT_LOCAL,
        IndexPlacement::Global => INDEX_PLACEMENT_GLOBAL,
    });
    out.push(match index.constraint {
        None => INDEX_CONSTRAINT_NONE,
        Some(IndexConstraint::PrimaryKey) => INDEX_CONSTRAINT_PRIMARY_KEY,
        Some(IndexConstraint::Unique) => INDEX_CONSTRAINT_UNIQUE,
    });
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
pub fn deserialize_index(bytes: &[u8]) -> Result<Index, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != INDEX_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown index version {version}"
        )));
    }
    let id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let name = read_string(&mut cur)?;
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let table = read_string(&mut cur)?;
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
    let constraint = match take_u8(&mut cur)? {
        INDEX_CONSTRAINT_NONE => None,
        INDEX_CONSTRAINT_PRIMARY_KEY => Some(IndexConstraint::PrimaryKey),
        INDEX_CONSTRAINT_UNIQUE => Some(IndexConstraint::Unique),
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
    Ok(Index {
        id,
        name,
        table,
        table_id,
        columns,
        unique,
        placement,
        constraint,
    })
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
pub fn deserialize_schema(
    bytes: &[u8],
) -> Result<(u32, Vec<Column>, TableOptions, Option<ForeignTableMeta>), KvError> {
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
        columns.push(Column {
            name,
            ty,
            not_null,
            default,
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
    Ok((table_id, columns, options, foreign))
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
#[must_use]
pub fn serialize_view(view: &View) -> Vec<u8> {
    let mut out = vec![VIEW_VERSION];
    write_str(&mut out, &view.name);
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
pub fn deserialize_view(bytes: &[u8]) -> Result<View, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != VIEW_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown view version {version}"
        )));
    }
    let name = read_string(&mut cur)?;
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

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (h, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated schema".into()))?;
    *cur = rest;
    Ok(*h)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
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
    use crate::{Column, ForeignTableMeta};

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
            },
            Column {
                name: "ratio".into(),
                ty: ColumnType::Numeric(None),
                not_null: false,
                default: None,
            },
            Column::new("code", ColumnType::Varchar(Some(8))),
            Column::new("flag", ColumnType::Char(Some(2))),
            Column::new("public_id", ColumnType::Uuid),
        ];
        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None);
        let (id, cols, options, foreign) = deserialize_schema(&bytes).expect("decode");
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
        }];

        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None);
        let (_id, decoded, _options, _foreign) = deserialize_schema(&bytes).expect("decode");

        assert_eq!(decoded, columns);
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
        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), None);
        let (id, cols, options, foreign) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(!options.sharded);
        assert!(foreign.is_none());
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
        let bytes = serialize_schema(table_id, &columns, TableOptions::default(), Some(&meta));
        let (id, cols, options, foreign) = deserialize_schema(&bytes).expect("decode");
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
    fn unknown_version_errors() {
        assert!(deserialize_schema(&[1, 0, 0, 0, 0]).is_err());
        assert!(deserialize_schema(&[99, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn ordinary_table_flag_zero_roundtrip() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(1, &columns, TableOptions::default(), None);
        let (_, _, options, foreign) = deserialize_schema(&bytes).expect("ordinary table decode");
        assert!(!options.sharded, "ordinary table has no sharded flag");
        assert!(foreign.is_none(), "ordinary table has no foreign meta");
    }

    #[test]
    fn sharded_option_roundtrips() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let bytes = serialize_schema(1, &columns, TableOptions { sharded: true }, None);
        let (_, _, options, foreign) = deserialize_schema(&bytes).expect("sharded decode");
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
        let index = Index {
            id: 7,
            name: "orders_email_idx".into(),
            table: "orders".into(),
            table_id: 3,
            columns: vec!["email".into()],
            unique: true,
            placement: IndexPlacement::Global,
            constraint: None,
        };

        let bytes = serialize_index(&index);

        assert_eq!(deserialize_index(&bytes).expect("index decode"), index);
    }

    #[test]
    fn unknown_flag_byte_errors() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let mut bytes = serialize_schema(1, &columns, TableOptions::default(), None);
        let last = bytes.last_mut().expect("foreign flag byte exists");
        *last = 2;
        assert!(deserialize_schema(&bytes).is_err());
    }

    #[test]
    fn unknown_table_option_flags_error() {
        let columns = vec![Column::new("x", ColumnType::Int4)];
        let mut bytes = serialize_schema(1, &columns, TableOptions::default(), None);
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
