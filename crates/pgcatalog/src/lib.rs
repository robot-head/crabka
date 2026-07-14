//! Catalog as a stateless view over a `Kv` store: tables, their columns, and
//! CRUD with `PostgreSQL` error codes. Persistence via SP3's KV layer.

#![doc(html_root_url = "https://docs.rs/crabka-pgcatalog/0.3.9")]

pub mod serde;

use std::collections::{BTreeMap, HashSet};

use crabka_pgkv::{Kv, KvError, WriteOp, key};
use crabka_pgtypes::{ColumnType, Datum};
use zerocopy::{
    FromBytes, IntoBytes,
    byteorder::big_endian::{U32, U64},
};

use crate::serde::{
    deserialize_fdw, deserialize_index, deserialize_schema, deserialize_sequence,
    deserialize_server, deserialize_sharding, deserialize_user_mapping, deserialize_view,
    serialize_fdw, serialize_index, serialize_schema, serialize_sequence, serialize_server,
    serialize_sharding, serialize_user_mapping, serialize_view,
};

/// OID-style table identifier (never 0; 0 is reserved/invalid).
pub type TableId = u32;

/// OID-style index identifier (never 0; 0 is reserved/invalid).
pub type IndexId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDefault {
    Value(Datum),
    NextVal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub not_null: bool,
    pub default: Option<ColumnDefault>,
}

impl Column {
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
            not_null: false,
            default: None,
        }
    }
}

/// Metadata stored alongside a foreign table that links it to its server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignTableMeta {
    /// The foreign server name this table is attached to.
    pub server: String,
    /// Table-level OPTIONS (e.g. `topic = 'orders'`).
    pub options: Vec<(String, String)>,
}

/// Ordinary-table creation options stored in the catalog schema record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableOptions {
    /// True when writes use global visibility and routing may span ranges.
    pub sharded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingStrategy {
    Hash(HashSharding),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashSharding {
    pub columns: Vec<String>,
    pub buckets: u32,
    pub co_location_group: Option<String>,
}

/// Physical ownership of a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPlacement {
    /// Index entries live with the base row's owning range.
    Local,
    /// Index entries live in separate index ranges and must ride timestamp txns.
    Global,
}

/// Constraint backed by an automatically-created index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexConstraint {
    PrimaryKey,
    Unique,
}

/// Secondary-index catalog definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub id: IndexId,
    pub name: String,
    pub table: String,
    pub table_id: TableId,
    pub columns: Vec<String>,
    pub unique: bool,
    pub placement: IndexPlacement,
    pub constraint: Option<IndexConstraint>,
}

/// Secondary-index catalog definition to create for a known table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub placement: IndexPlacement,
    pub constraint: Option<IndexConstraint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sequence {
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cache: i64,
    pub cycle: bool,
    pub last_value: i64,
    pub is_called: bool,
}

impl Sequence {
    #[must_use]
    pub fn new(
        start: i64,
        increment: i64,
        min: Option<i64>,
        max: Option<i64>,
        cache: Option<i64>,
        cycle: bool,
    ) -> Self {
        let min = min.unwrap_or(if increment > 0 { 1 } else { i64::MIN });
        let max = max.unwrap_or(if increment > 0 { i64::MAX } else { -1 });
        Self {
            start,
            increment,
            min,
            max,
            cache: cache.unwrap_or(1),
            cycle,
            last_value: start,
            is_called: false,
        }
    }
}

/// A foreign-data wrapper registration (`CREATE FOREIGN DATA WRAPPER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignDataWrapper {
    pub name: String,
    /// OPTIONS (e.g. handler, validator).
    pub options: Vec<(String, String)>,
}

/// A foreign server registration (`CREATE SERVER … FOREIGN DATA WRAPPER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignServer {
    pub name: String,
    /// The FDW this server belongs to.
    pub wrapper: String,
    /// Server-level OPTIONS (e.g. `bootstrap_servers`).
    pub options: Vec<(String, String)>,
}

/// A user mapping (`CREATE USER MAPPING FOR … SERVER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserMapping {
    pub user: String,
    pub server: String,
    /// Mapping-level OPTIONS.
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    pub can_login: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePrivilege {
    pub table: String,
    pub grantee: String,
    pub privilege: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    /// True when the table uses global-visibility semantics and may span ranges.
    pub sharded: bool,
    /// Optional physical sharding strategy for range routing.
    pub sharding: Option<ShardingStrategy>,
    /// Present when the table is a foreign table; `None` for ordinary tables.
    pub foreign: Option<ForeignTableMeta>,
}

/// A stored view definition and its resolved output schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub name: String,
    pub definition: String,
    pub columns: Vec<Column>,
}

impl Table {
    /// Zero-based ordinal of a column by name, or None.
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("relation \"{0}\" already exists")]
    DuplicateTable(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("\"{0}\" is not a view")]
    WrongObjectType(String),
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),
    #[error("index \"{0}\" already exists")]
    DuplicateIndex(String),
    #[error("index \"{0}\" does not exist")]
    UndefinedIndex(String),
    #[error("cannot drop index \"{0}\" because it is required by a table constraint")]
    DependentObjectsStillExist(String),
    #[error("sequence \"{0}\" already exists")]
    DuplicateSequence(String),
    #[error("sequence \"{0}\" does not exist")]
    UndefinedSequence(String),
    #[error("invalid sequence definition: {0}")]
    InvalidSequence(String),
    #[error("relation \"{0}\" is not an ordinary table")]
    NotOrdinaryTable(String),
    #[error("table conversion rewrite does not remove every existing physical tuple")]
    IncompleteConversionRewrite,
    #[error("cannot rename relation \"{0}\" while stored views exist")]
    StoredViewDependency(String),
    /// Generic "object already exists" (42710) — for FDW, server, user-mapping.
    #[error("object \"{0}\" already exists")]
    DuplicateObject(String),
    /// Generic "undefined object" (42704) — for FDW, server, user-mapping.
    #[error("object \"{0}\" does not exist")]
    UndefinedObject(String),
    #[error("catalog storage error: {0}")]
    Storage(#[from] KvError),
}

impl CatalogError {
    #[must_use]
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_)
            | CatalogError::DuplicateIndex(_)
            | CatalogError::DuplicateSequence(_) => "42P07",
            CatalogError::UndefinedTable(_) | CatalogError::UndefinedSequence(_) => "42P01",
            CatalogError::WrongObjectType(_) => "42809",
            CatalogError::UndefinedColumn(_) => "42703",
            CatalogError::UndefinedIndex(_) | CatalogError::UndefinedObject(_) => "42704",
            CatalogError::DependentObjectsStillExist(_) => "2BP01",
            CatalogError::InvalidSequence(_) => "22023",
            CatalogError::NotOrdinaryTable(_) | CatalogError::StoredViewDependency(_) => "0A000",
            CatalogError::DuplicateObject(_) => "42710",
            CatalogError::Storage(KvError::Io(_)) => "58030",
            CatalogError::IncompleteConversionRewrite
            | CatalogError::Storage(
                KvError::CorruptRow(_)
                | KvError::RestoreTargetNotEmpty
                | KvError::UnsortedSnapshot
                | KvError::ConditionalPutUnsupported,
            ) => "XX000",
        }
    }
}

/// Build the atomic catalog batch for renaming an ordinary or foreign table.
///
/// Rows and local secondary-index entries are keyed by immutable IDs, so their
/// physical keys do not move. Index *metadata* and table privileges carry the
/// table name and are rewritten in the same batch. Index names are preserved.
/// Stored views retain SQL text rather than dependency identities; until that
/// representation can be rewritten safely, any stored view blocks a rename.
///
/// # Errors
///
/// Returns missing/wrong-type/duplicate-relation, stored-view-dependency, or
/// storage/corruption errors from the catalog KV seam.
pub fn rename_table_ops(
    kv: &dyn Kv,
    name: &str,
    new_name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let schema = match kv.get(&key::catalog_key(name))? {
        Some(schema) => schema,
        None if kv.get(&view_key(name))?.is_some() => {
            return Err(CatalogError::WrongObjectType(name.to_string()));
        }
        None => return Err(CatalogError::UndefinedTable(name.to_string())),
    };
    if relation_exists(kv, new_name)? {
        return Err(CatalogError::DuplicateTable(new_name.to_string()));
    }
    if has_stored_views(kv)? {
        return Err(CatalogError::StoredViewDependency(name.to_string()));
    }

    let (table_id, _, _, _) = deserialize_schema(&schema)?;
    let mut ops = vec![
        WriteOp::Delete {
            key: key::catalog_key(name),
        },
        WriteOp::Put {
            key: key::catalog_key(new_name),
            value: schema,
        },
    ];
    if let Some(sharding) = kv.get(&key::catalog_sharding_key(name))? {
        ops.push(WriteOp::Delete {
            key: key::catalog_sharding_key(name),
        });
        ops.push(WriteOp::Put {
            key: key::catalog_sharding_key(new_name),
            value: sharding,
        });
    }
    for (table_index_key, index_bytes) in kv.scan_prefix(&catalog_table_index_prefix(table_id))? {
        let mut index = deserialize_index(&index_bytes)?;
        index.table = new_name.to_string();
        let renamed_index = serialize_index(&index);
        ops.push(WriteOp::Put {
            key: catalog_index_key(&index.name),
            value: renamed_index.clone(),
        });
        ops.push(WriteOp::Put {
            key: table_index_key,
            value: renamed_index,
        });
    }
    for (privilege_key, privilege) in scan_table_privileges(kv)? {
        if privilege.table != name {
            continue;
        }
        ops.push(WriteOp::Delete { key: privilege_key });
        ops.push(WriteOp::Put {
            key: table_privilege_key(new_name, &privilege.grantee, &privilege.privilege),
            value: serialize_table_privilege(new_name, &privilege.grantee, &privilege.privilege),
        });
    }
    Ok(ops)
}

fn has_stored_views(kv: &dyn Kv) -> Result<bool, CatalogError> {
    let mut prefix = key::catalog_key("");
    prefix.extend_from_slice(b"\0view/");
    Ok(!kv.scan_prefix(&prefix)?.is_empty())
}

/// Build the write batch for creating a table (schema + sequence init +
/// `next_table_id` bump) WITHOUT writing — caller persists the ops. Returns the
/// allocated `TableId` alongside the batch. Used by the executor so DDL writes can
/// be routed through the durable-write seam (and replicated). Validation
/// (duplicate-table check, `next_table_id` read) is identical to `create_table`.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_ops(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    create_table_with_options_ops(kv, name, columns, TableOptions::default())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves the donor catalog API shape consumed by executor DDL paths"
)]
/// Build the write batch for creating a table with explicit metadata options.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_with_options_ops(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
    options: TableOptions,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    if relation_exists(kv, name)? {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let next = read_next_table_id(kv)?;
    let batch = vec![
        WriteOp::Put {
            key: key::catalog_key(name),
            value: serialize_schema(next, &columns, options, None),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: U64::new(1).as_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: U32::new(next + 1).as_bytes().to_vec(),
        },
    ];
    Ok((next, batch))
}

/// Build the write batch that creates a view without persisting it.
///
/// # Errors
///
/// Returns duplicate-relation or storage/corruption errors from the catalog KV seam.
pub fn create_view_ops(
    kv: &dyn Kv,
    name: &str,
    definition: String,
    columns: Vec<Column>,
) -> Result<Vec<WriteOp>, CatalogError> {
    if relation_exists(kv, name)? {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let view = View {
        name: name.to_string(),
        definition,
        columns,
    };
    Ok(vec![WriteOp::Put {
        key: view_key(name),
        value: serialize_view(&view),
    }])
}

/// Create a view and its output schema in one atomic batch.
///
/// # Errors
///
/// Returns duplicate-relation or storage/corruption errors from the catalog KV seam.
pub fn create_view(
    kv: &dyn Kv,
    name: &str,
    definition: String,
    columns: Vec<Column>,
) -> Result<(), CatalogError> {
    kv.write_batch(&create_view_ops(kv, name, definition, columns)?)?;
    Ok(())
}

/// Look up a view by relation name.
///
/// # Errors
///
/// Returns undefined-relation or storage/corruption errors from the catalog KV seam.
pub fn get_view(kv: &dyn Kv, name: &str) -> Result<View, CatalogError> {
    let bytes = kv
        .get(&view_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    deserialize_view(&bytes).map_err(CatalogError::from)
}

/// Build the write batch that drops a view without persisting it.
///
/// # Errors
///
/// Returns undefined-relation or storage/corruption errors from the catalog KV seam.
pub fn drop_view_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&view_key(name))?.is_some() {
        return Ok(vec![WriteOp::Delete {
            key: view_key(name),
        }]);
    }
    if kv.get(&key::catalog_key(name))?.is_some() {
        return Err(CatalogError::WrongObjectType(name.to_string()));
    }
    Err(CatalogError::UndefinedTable(name.to_string()))
}

/// Drop a view in one atomic batch.
///
/// # Errors
///
/// Returns undefined-relation or storage/corruption errors from the catalog KV seam.
pub fn drop_view(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    kv.write_batch(&drop_view_ops(kv, name)?)?;
    Ok(())
}

/// Build the write batch for creating a table and optional sharding metadata.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_with_sharding_ops(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
    options: TableOptions,
    sharding: Option<&ShardingStrategy>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    if let Some(ShardingStrategy::Hash(hash)) = sharding {
        validate_hash_sharding_column_defs(&columns, hash)?;
    }
    let (table_id, mut ops) = create_table_with_options_ops(kv, name, columns, options)?;
    if let Some(strategy) = sharding {
        ops.push(WriteOp::Put {
            key: key::catalog_sharding_key(name),
            value: serialize_sharding(Some(strategy)),
        });
    }
    Ok((table_id, ops))
}

/// Create a table: allocate a `TableId`, persist the schema, init the sequence —
/// all in one atomic batch. Caller serializes concurrent DDL.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_table_ops(kv, name, columns)?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Create a table with explicit metadata options.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_with_options(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
    options: TableOptions,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_table_with_options_ops(kv, name, columns, options)?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Look up a table by name.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn get_table(kv: &dyn Kv, name: &str) -> Result<Table, CatalogError> {
    let bytes = kv
        .get(&key::catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, columns, options, foreign) = deserialize_schema(&bytes)?;
    let sharding = kv
        .get(&key::catalog_sharding_key(name))?
        .map(|bytes| deserialize_sharding(&bytes))
        .transpose()?
        .flatten();
    Ok(Table {
        id,
        name: name.to_string(),
        columns,
        sharded: options.sharded,
        sharding,
        foreign,
    })
}

/// Return every ordinary/foreign table in catalog-name order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_tables(kv: &dyn Kv) -> Result<Vec<Table>, CatalogError> {
    let prefix = key::catalog_key("");
    let mut tables = kv
        .scan_prefix(&prefix)?
        .into_iter()
        .filter_map(|(table_key, bytes)| {
            table_name_from_catalog_key(&prefix, &table_key)
                .map(|name| table_from_schema_bytes(kv, name, &bytes))
        })
        .collect::<Result<Vec<_>, _>>()?;
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

fn table_name_from_catalog_key<'a>(prefix: &[u8], table_key: &'a [u8]) -> Option<&'a str> {
    let suffix = table_key.strip_prefix(prefix)?;
    if suffix.contains(&b'/') {
        return None;
    }
    std::str::from_utf8(suffix).ok()
}

fn view_key(name: &str) -> Vec<u8> {
    let mut key = key::catalog_key("");
    key.extend_from_slice(b"\0view/");
    key.extend_from_slice(name.as_bytes());
    key
}

fn relation_exists(kv: &dyn Kv, name: &str) -> Result<bool, CatalogError> {
    Ok(kv.get(&key::catalog_key(name))?.is_some()
        || kv.get(&view_key(name))?.is_some()
        || kv.get(&catalog_sequence_key(name))?.is_some())
}

fn table_from_schema_bytes(kv: &dyn Kv, name: &str, bytes: &[u8]) -> Result<Table, CatalogError> {
    let (id, columns, options, foreign) = deserialize_schema(bytes)?;
    let sharding = kv
        .get(&key::catalog_sharding_key(name))?
        .map(|bytes| deserialize_sharding(&bytes))
        .transpose()?
        .flatten();
    Ok(Table {
        id,
        name: name.to_string(),
        columns,
        sharded: options.sharded,
        sharding,
        foreign,
    })
}

/// Return a table's optional hash-sharding strategy metadata.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn get_table_sharding(
    kv: &dyn Kv,
    name: &str,
) -> Result<Option<ShardingStrategy>, CatalogError> {
    let _table = get_table(kv, name)?;
    let Some(bytes) = kv.get(&key::catalog_sharding_key(name))? else {
        return Ok(None);
    };

    Ok(deserialize_sharding(&bytes)?)
}

/// Build a write batch that replaces a table's optional sharding metadata.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn set_table_sharding_ops(
    kv: &dyn Kv,
    name: &str,
    sharding: Option<&ShardingStrategy>,
) -> Result<Vec<WriteOp>, CatalogError> {
    let table = get_table(kv, name)?;
    if let Some(ShardingStrategy::Hash(hash)) = sharding {
        validate_hash_sharding_columns(&table, hash)?;
    }
    let key = key::catalog_sharding_key(name);
    let op = match sharding {
        None => WriteOp::Delete { key },
        Some(strategy) => WriteOp::Put {
            key,
            value: serialize_sharding(Some(strategy)),
        },
    };
    Ok(vec![op])
}

/// Complete a table conversion batch by atomically publishing sharded visibility
/// and replacing optional physical sharding metadata.
///
/// `rewrite_ops` must contain the complete physical data transition for the
/// table. Callers must commit the returned batch as one unit; publishing this
/// metadata without the rewrite makes existing xid-MVCC rows unreadable to
/// timestamp scans.
///
/// # Errors
///
/// Returns undefined-table, unsupported foreign-table conversion,
/// undefined-column, or storage/corruption errors from the catalog KV seam.
pub fn complete_table_conversion_ops(
    kv: &dyn Kv,
    name: &str,
    sharding: Option<&ShardingStrategy>,
    mut rewrite_ops: Vec<WriteOp>,
) -> Result<Vec<WriteOp>, CatalogError> {
    let bytes = kv
        .get(&key::catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, columns, options, foreign) = deserialize_schema(&bytes)?;
    if foreign.is_some() {
        return Err(CatalogError::NotOrdinaryTable(name.to_string()));
    }
    let table = Table {
        id,
        name: name.to_string(),
        columns,
        sharded: options.sharded,
        sharding: get_table_sharding(kv, name)?,
        foreign: None,
    };
    if let Some(ShardingStrategy::Hash(hash)) = sharding {
        validate_hash_sharding_columns(&table, hash)?;
    }
    validate_conversion_rewrite(kv, table.id, &rewrite_ops)?;

    rewrite_ops.push(WriteOp::Put {
        key: key::catalog_key(name),
        value: serialize_schema(id, &table.columns, TableOptions { sharded: true }, None),
    });
    rewrite_ops.extend(set_table_sharding_ops(kv, name, sharding)?);
    Ok(rewrite_ops)
}

fn validate_conversion_rewrite(
    kv: &dyn Kv,
    table_id: TableId,
    rewrite_ops: &[WriteOp],
) -> Result<(), CatalogError> {
    let table_prefix = key::table_prefix(table_id);
    let mut final_tuples = kv
        .scan_prefix(&key::table_prefix(table_id))?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let table_was_empty = final_tuples.is_empty();
    if table_was_empty
        && !rewrite_ops
            .iter()
            .any(|op| matches!(op, WriteOp::Delete { key } if *key == table_prefix))
    {
        // An empty table still needs the executor's explicit physical-rewrite
        // proof. The prefix cannot be a tuple key because it lacks a rowid.
        return Err(CatalogError::IncompleteConversionRewrite);
    }

    for op in rewrite_ops {
        match op {
            WriteOp::Delete { key } if key.starts_with(&table_prefix) => {
                final_tuples.remove(key);
            }
            WriteOp::Put { key, value } | WriteOp::ConditionalPut { key, value, .. }
                if key.starts_with(&table_prefix) =>
            {
                final_tuples.insert(key.clone(), value.clone());
            }
            WriteOp::Put { .. } | WriteOp::ConditionalPut { .. } | WriteOp::Delete { .. } => {}
        }
    }

    if final_tuples
        .values()
        .all(|value| crabka_pgmvcc::version::decode_ts_tuple(value).is_ok())
    {
        return Ok(());
    }
    Err(CatalogError::IncompleteConversionRewrite)
}

fn catalog_index_key(name: &str) -> Vec<u8> {
    let mut out = b"\0\0\0\0catalog/index/by-name/".to_vec();
    out.extend_from_slice(name.as_bytes());
    out
}

fn catalog_index_prefix() -> Vec<u8> {
    b"\0\0\0\0catalog/index/by-name/".to_vec()
}

fn catalog_table_index_key(table_id: TableId, index_name: &str) -> Vec<u8> {
    let mut out = b"\0\0\0\0catalog/index/by-table/".to_vec();
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(b"/");
    out.extend_from_slice(index_name.as_bytes());
    out
}

fn catalog_table_index_prefix(table_id: TableId) -> Vec<u8> {
    let mut out = b"\0\0\0\0catalog/index/by-table/".to_vec();
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(b"/");
    out
}

fn meta_next_index_id_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_index_id".to_vec()
}

fn read_next_index_id(kv: &dyn Kv) -> Result<IndexId, CatalogError> {
    match kv.get(&meta_next_index_id_key())? {
        Some(bytes) => {
            let (id, _) = U32::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("next_index_id is not u32".into()))?;
            Ok(id.get())
        }
        None => Ok(1),
    }
}

/// Build the write batch for creating a secondary-index catalog record.
///
/// # Errors
///
/// Returns duplicate-index, undefined-table/column, or storage/corruption errors
/// from the catalog KV seam.
pub fn create_index_ops(
    kv: &dyn Kv,
    name: &str,
    table: &str,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
) -> Result<(IndexId, Vec<WriteOp>), CatalogError> {
    if kv.get(&catalog_index_key(name))?.is_some() {
        return Err(CatalogError::DuplicateIndex(name.to_string()));
    }
    let table_meta = get_table(kv, table)?;
    validate_index_columns(&table_meta, &columns)?;
    let id = read_next_index_id(kv)?;
    let index = Index {
        id,
        name: name.to_string(),
        table: table.to_string(),
        table_id: table_meta.id,
        columns,
        unique,
        placement,
        constraint: None,
    };
    let value = serialize_index(&index);
    let ops = vec![
        WriteOp::Put {
            key: catalog_index_key(name),
            value: value.clone(),
        },
        WriteOp::Put {
            key: catalog_table_index_key(table_meta.id, name),
            value,
        },
        WriteOp::Put {
            key: meta_next_index_id_key(),
            value: U32::new(id + 1).as_bytes().to_vec(),
        },
    ];
    Ok((id, ops))
}

/// Build the write batch for creating a secondary-index catalog record on a
/// table that may not be visible in the catalog KV yet.
///
/// # Errors
///
/// Returns duplicate-index, undefined-column, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_index_on_table_ops(
    kv: &dyn Kv,
    table: &Table,
    name: &str,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
) -> Result<(IndexId, Vec<WriteOp>), CatalogError> {
    if kv.get(&catalog_index_key(name))?.is_some() {
        return Err(CatalogError::DuplicateIndex(name.to_string()));
    }
    validate_index_columns(table, &columns)?;
    let id = read_next_index_id(kv)?;
    let index = Index {
        id,
        name: name.to_string(),
        table: table.name.clone(),
        table_id: table.id,
        columns,
        unique,
        placement,
        constraint: None,
    };
    let value = serialize_index(&index);
    let ops = vec![
        WriteOp::Put {
            key: catalog_index_key(name),
            value: value.clone(),
        },
        WriteOp::Put {
            key: catalog_table_index_key(table.id, name),
            value,
        },
        WriteOp::Put {
            key: meta_next_index_id_key(),
            value: U32::new(id + 1).as_bytes().to_vec(),
        },
    ];
    Ok((id, ops))
}

/// Build the write batch for creating secondary-index catalog records on a
/// table that may not be visible in the catalog KV yet.
///
/// # Errors
///
/// Returns duplicate-index, undefined-column, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_indexes_on_table_ops(
    kv: &dyn Kv,
    table: &Table,
    indexes: &[NewIndex],
) -> Result<Vec<WriteOp>, CatalogError> {
    if indexes.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen_names = HashSet::with_capacity(indexes.len());
    for index in indexes {
        if !seen_names.insert(index.name.as_str())
            || kv.get(&catalog_index_key(&index.name))?.is_some()
        {
            return Err(CatalogError::DuplicateIndex(index.name.clone()));
        }
        validate_index_columns(table, &index.columns)?;
    }

    let first_id = read_next_index_id(kv)?;
    let mut ops = Vec::with_capacity(indexes.len() * 2 + 1);
    for (offset, new_index) in indexes.iter().enumerate() {
        let id = first_id
            + u32::try_from(offset).map_err(|_| {
                CatalogError::Storage(KvError::CorruptRow("too many indexes in one batch".into()))
            })?;
        let index = Index {
            id,
            name: new_index.name.clone(),
            table: table.name.clone(),
            table_id: table.id,
            columns: new_index.columns.clone(),
            unique: new_index.unique,
            placement: new_index.placement,
            constraint: new_index.constraint,
        };
        let value = serialize_index(&index);
        ops.push(WriteOp::Put {
            key: catalog_index_key(&index.name),
            value: value.clone(),
        });
        ops.push(WriteOp::Put {
            key: catalog_table_index_key(table.id, &index.name),
            value,
        });
    }
    let index_count = u32::try_from(indexes.len()).map_err(|_| {
        CatalogError::Storage(KvError::CorruptRow("too many indexes in one batch".into()))
    })?;
    ops.push(WriteOp::Put {
        key: meta_next_index_id_key(),
        value: U32::new(first_id + index_count).as_bytes().to_vec(),
    });
    Ok(ops)
}

/// Persist a secondary-index catalog record.
///
/// # Errors
///
/// Returns duplicate-index, undefined-table/column, or storage/corruption errors.
pub fn create_index(
    kv: &dyn Kv,
    name: &str,
    table: &str,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
) -> Result<IndexId, CatalogError> {
    let (id, ops) = create_index_ops(kv, name, table, columns, unique, placement)?;
    kv.write_batch(&ops)?;
    Ok(id)
}

/// Look up a secondary index by name.
///
/// # Errors
///
/// Returns undefined-index or storage/corruption errors.
pub fn get_index(kv: &dyn Kv, name: &str) -> Result<Index, CatalogError> {
    let bytes = kv
        .get(&catalog_index_key(name))?
        .ok_or_else(|| CatalogError::UndefinedIndex(name.to_string()))?;
    Ok(deserialize_index(&bytes)?)
}

/// Build the metadata write batch for dropping an index without persisting it.
///
/// The returned definition lets the executor remove corresponding local-index
/// entries in the same durable write batch.
///
/// # Errors
///
/// Returns undefined-index, wrong-object-type, dependent-object, or storage
/// errors from the catalog KV seam.
pub fn drop_index_ops(kv: &dyn Kv, name: &str) -> Result<(Index, Vec<WriteOp>), CatalogError> {
    let index = match get_index(kv, name) {
        Ok(index) => index,
        Err(CatalogError::UndefinedIndex(_)) if relation_exists(kv, name)? => {
            return Err(CatalogError::WrongObjectType(name.to_string()));
        }
        Err(error) => return Err(error),
    };
    if index.constraint.is_some() {
        return Err(CatalogError::DependentObjectsStillExist(name.to_string()));
    }
    Ok((
        index.clone(),
        vec![
            WriteOp::Delete {
                key: catalog_index_key(name),
            },
            WriteOp::Delete {
                key: catalog_table_index_key(index.table_id, name),
            },
        ],
    ))
}

/// Return every secondary-index catalog record, sorted by index name.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_indexes(kv: &dyn Kv) -> Result<Vec<Index>, CatalogError> {
    let mut indexes = kv
        .scan_prefix(&catalog_index_prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_index(&bytes).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

fn catalog_sequence_key(name: &str) -> Vec<u8> {
    let mut out = b"\0\0\0\0catalog/sequence/by-name/".to_vec();
    out.extend_from_slice(name.as_bytes());
    out
}

/// Build the write batch for creating a sequence catalog record.
///
/// # Errors
///
/// Returns duplicate-sequence, invalid-sequence, or storage/corruption errors.
pub fn create_sequence_ops(
    kv: &dyn Kv,
    name: &str,
    sequence: Sequence,
) -> Result<Vec<WriteOp>, CatalogError> {
    validate_sequence(sequence)?;
    if kv.get(&catalog_sequence_key(name))?.is_some() {
        return Err(CatalogError::DuplicateSequence(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: catalog_sequence_key(name),
        value: serialize_sequence(sequence),
    }])
}

/// Look up a sequence by name.
///
/// # Errors
///
/// Returns undefined-sequence or storage/corruption errors.
pub fn get_sequence(kv: &dyn Kv, name: &str) -> Result<Sequence, CatalogError> {
    let bytes = kv
        .get(&catalog_sequence_key(name))?
        .ok_or_else(|| CatalogError::UndefinedSequence(name.to_string()))?;
    Ok(deserialize_sequence(&bytes)?)
}

/// Replace a sequence record.
#[must_use]
pub fn put_sequence_op(name: &str, sequence: Sequence) -> WriteOp {
    WriteOp::Put {
        key: catalog_sequence_key(name),
        value: serialize_sequence(sequence),
    }
}

/// Build the write batch for dropping a sequence.
///
/// # Errors
///
/// Returns undefined-sequence or storage/corruption errors.
pub fn drop_sequence_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_sequence(kv, name)?;
    Ok(vec![WriteOp::Delete {
        key: catalog_sequence_key(name),
    }])
}

fn validate_sequence(sequence: Sequence) -> Result<(), CatalogError> {
    if sequence.increment == 0 {
        return Err(CatalogError::InvalidSequence(
            "INCREMENT must not be zero".into(),
        ));
    }
    if sequence.cache <= 0 {
        return Err(CatalogError::InvalidSequence(
            "CACHE must be greater than zero".into(),
        ));
    }
    if sequence.min > sequence.max {
        return Err(CatalogError::InvalidSequence(
            "MINVALUE must be less than or equal to MAXVALUE".into(),
        ));
    }
    if sequence.start < sequence.min || sequence.start > sequence.max {
        return Err(CatalogError::InvalidSequence(
            "START value is outside sequence bounds".into(),
        ));
    }
    Ok(())
}

/// Return indexes declared on a table, sorted by index name.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors.
pub fn list_table_indexes(kv: &dyn Kv, table: &str) -> Result<Vec<Index>, CatalogError> {
    let table = get_table(kv, table)?;
    let mut indexes = kv
        .scan_prefix(&catalog_table_index_prefix(table.id))?
        .into_iter()
        .map(|(_, bytes)| deserialize_index(&bytes).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

fn validate_index_columns(table: &Table, columns: &[String]) -> Result<(), CatalogError> {
    if columns.is_empty() {
        return Err(CatalogError::UndefinedColumn(String::new()));
    }
    for column in columns {
        if table.column_index(column).is_none() {
            return Err(CatalogError::UndefinedColumn(column.clone()));
        }
    }
    Ok(())
}

fn validate_hash_sharding_columns(table: &Table, hash: &HashSharding) -> Result<(), CatalogError> {
    for column in &hash.columns {
        if table.column_index(column).is_none() {
            return Err(CatalogError::UndefinedColumn(column.clone()));
        }
    }
    Ok(())
}

fn validate_hash_sharding_column_defs(
    columns: &[Column],
    hash: &HashSharding,
) -> Result<(), CatalogError> {
    for hash_column in &hash.columns {
        if !columns.iter().any(|column| column.name == *hash_column) {
            return Err(CatalogError::UndefinedColumn(hash_column.clone()));
        }
    }
    Ok(())
}

/// Build the write batch for dropping a table (catalog entry + sequence + every
/// row) WITHOUT writing — caller persists the ops. Errors (42P01 on a missing
/// table) are identical to `drop_table`. Used by the executor to route DDL
/// writes through the durable-write seam.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn drop_table_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let table = get_table(kv, name)?;
    let mut ops = vec![
        WriteOp::Delete {
            key: key::catalog_key(name),
        },
        WriteOp::Delete {
            key: key::seq_key(table.id),
        },
    ];
    for (row_key, _) in kv.scan_prefix(&key::table_prefix(table.id))? {
        ops.push(WriteOp::Delete { key: row_key });
    }
    for (index_table_key, index_bytes) in kv.scan_prefix(&catalog_table_index_prefix(table.id))? {
        let index = deserialize_index(&index_bytes)?;
        ops.push(WriteOp::Delete {
            key: catalog_index_key(&index.name),
        });
        ops.push(WriteOp::Delete {
            key: index_table_key,
        });
    }
    Ok(ops)
}

/// Drop a table: delete the catalog entry, the sequence, and all its rows — one
/// atomic batch.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn drop_table(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_table_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

// ── Roles and table privileges ────────────────────────────────────────────────

const ROLE_PREFIX: &[u8] = b"catalog/role/";
const TABLE_PRIVILEGE_PREFIX: &[u8] = b"catalog/table_privilege/";

/// Create a role or login-capable user metadata row.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_role(kv: &dyn Kv, name: &str, can_login: bool) -> Result<(), CatalogError> {
    let ops = create_role_ops(kv, name, can_login)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for creating a role without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_role_ops(
    kv: &dyn Kv,
    name: &str,
    can_login: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    if role_exists(kv, name)? {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: role_key(name),
        value: serialize_role(name, can_login),
    }])
}

/// Look up a role by name.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_role(kv: &dyn Kv, name: &str) -> Result<Role, CatalogError> {
    if name == "public" {
        return Ok(Role {
            name: "public".into(),
            can_login: true,
        });
    }
    let bytes = kv
        .get(&role_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    deserialize_role(&bytes)
}

/// Return whether a role exists.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn role_exists(kv: &dyn Kv, name: &str) -> Result<bool, CatalogError> {
    if name == "public" {
        return Ok(true);
    }
    Ok(kv.get(&role_key(name))?.is_some())
}

/// List roles, including the built-in starter `public` session user.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_roles(kv: &dyn Kv) -> Result<Vec<Role>, CatalogError> {
    let mut roles = vec![Role {
        name: "public".into(),
        can_login: true,
    }];
    for (_, bytes) in kv.scan_prefix(ROLE_PREFIX)? {
        let role = deserialize_role(&bytes)?;
        if role.name != "public" {
            roles.push(role);
        }
    }
    roles.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(roles)
}

/// Drop a role metadata row.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_role(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_role_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a role without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_role_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    if name == "public" {
        return Err(CatalogError::UndefinedObject(name.to_string()));
    }
    let _ = get_role(kv, name)?;
    let mut ops = vec![WriteOp::Delete {
        key: role_key(name),
    }];
    for (key, privilege) in scan_table_privileges(kv)? {
        if privilege.grantee == name {
            ops.push(WriteOp::Delete { key });
        }
    }
    Ok(ops)
}

/// Build write ops for recording table privilege grants.
///
/// # Errors
///
/// Returns undefined-table, undefined-object, or storage/corruption errors.
pub fn grant_table_privileges_ops(
    kv: &dyn Kv,
    table: &str,
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_table(kv, table)?;
    let mut ops = Vec::new();
    for grantee in grantees {
        let _ = get_role(kv, grantee)?;
        for privilege in privileges {
            ops.push(WriteOp::Put {
                key: table_privilege_key(table, grantee, privilege),
                value: serialize_table_privilege(table, grantee, privilege),
            });
        }
    }
    Ok(ops)
}

/// Build write ops for removing recorded table privilege grants.
///
/// # Errors
///
/// Returns undefined-table, undefined-object, or storage/corruption errors.
pub fn revoke_table_privileges_ops(
    kv: &dyn Kv,
    table: &str,
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_table(kv, table)?;
    let mut ops = Vec::new();
    for grantee in grantees {
        let _ = get_role(kv, grantee)?;
        for privilege in privileges {
            ops.push(WriteOp::Delete {
                key: table_privilege_key(table, grantee, privilege),
            });
        }
    }
    Ok(ops)
}

/// List recorded table privileges.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_table_privileges(kv: &dyn Kv) -> Result<Vec<TablePrivilege>, CatalogError> {
    scan_table_privileges(kv).map(|entries| {
        entries
            .into_iter()
            .map(|(_, privilege)| privilege)
            .collect()
    })
}

fn scan_table_privileges(kv: &dyn Kv) -> Result<Vec<(Vec<u8>, TablePrivilege)>, CatalogError> {
    kv.scan_prefix(TABLE_PRIVILEGE_PREFIX)?
        .into_iter()
        .map(|(key, bytes)| Ok((key, deserialize_table_privilege(&bytes)?)))
        .collect()
}

fn role_key(name: &str) -> Vec<u8> {
    let mut key = ROLE_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn table_privilege_key(table: &str, grantee: &str, privilege: &str) -> Vec<u8> {
    let mut key = TABLE_PRIVILEGE_PREFIX.to_vec();
    key.extend_from_slice(table.as_bytes());
    key.push(0);
    key.extend_from_slice(grantee.as_bytes());
    key.push(0);
    key.extend_from_slice(privilege.as_bytes());
    key
}

fn serialize_role(name: &str, can_login: bool) -> Vec<u8> {
    let mut bytes = vec![1, u8::from(can_login)];
    bytes.extend_from_slice(name.as_bytes());
    bytes
}

fn deserialize_role(bytes: &[u8]) -> Result<Role, CatalogError> {
    if bytes.len() < 2 || bytes[0] != 1 {
        return Err(KvError::CorruptRow("role record has invalid version".into()).into());
    }
    let name = std::str::from_utf8(&bytes[2..])
        .map_err(|_| KvError::CorruptRow("role name is not utf8".into()))?
        .to_string();
    Ok(Role {
        name,
        can_login: bytes[1] != 0,
    })
}

fn serialize_table_privilege(table: &str, grantee: &str, privilege: &str) -> Vec<u8> {
    [table, grantee, privilege].join("\0").into_bytes()
}

fn deserialize_table_privilege(bytes: &[u8]) -> Result<TablePrivilege, CatalogError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| KvError::CorruptRow("table privilege is not utf8".into()))?;
    let parts = text.split('\0').collect::<Vec<_>>();
    if let [table, grantee, privilege] = parts.as_slice() {
        return Ok(TablePrivilege {
            table: (*table).to_string(),
            grantee: (*grantee).to_string(),
            privilege: (*privilege).to_string(),
        });
    }
    Err(KvError::CorruptRow("table privilege has invalid shape".into()).into())
}

// ── Foreign-data wrapper ──────────────────────────────────────────────────────

/// Register a foreign-data wrapper.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_fdw(
    kv: &dyn Kv,
    name: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    let ops = create_fdw_ops(kv, name, options)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for registering a foreign-data wrapper without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves donor CRUD API ownership for metadata options"
)]
pub fn create_fdw_ops(
    kv: &dyn Kv,
    name: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&key::fdw_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: key::fdw_key(name),
        value: serialize_fdw(name, &options),
    }])
}

/// Look up a foreign-data wrapper by name.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_fdw(kv: &dyn Kv, name: &str) -> Result<ForeignDataWrapper, CatalogError> {
    let bytes = kv
        .get(&key::fdw_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    Ok(deserialize_fdw(&bytes)?)
}

/// Drop a foreign-data wrapper.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_fdw(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_fdw_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a foreign-data wrapper without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_fdw_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_fdw(kv, name)?;
    Ok(vec![WriteOp::Delete {
        key: key::fdw_key(name),
    }])
}

// ── Foreign server ────────────────────────────────────────────────────────────

/// Register a foreign server.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_server(
    kv: &dyn Kv,
    name: &str,
    wrapper: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    let ops = create_server_ops(kv, name, wrapper, options)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for registering a foreign server without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves donor CRUD API ownership for metadata options"
)]
pub fn create_server_ops(
    kv: &dyn Kv,
    name: &str,
    wrapper: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&key::server_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: key::server_key(name),
        value: serialize_server(name, wrapper, &options),
    }])
}

/// Look up a foreign server by name.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_server(kv: &dyn Kv, name: &str) -> Result<ForeignServer, CatalogError> {
    let bytes = kv
        .get(&key::server_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    Ok(deserialize_server(&bytes)?)
}

/// Drop a foreign server.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_server(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_server_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a foreign server without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_server_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_server(kv, name)?;
    Ok(vec![WriteOp::Delete {
        key: key::server_key(name),
    }])
}

// ── User mapping ──────────────────────────────────────────────────────────────

/// Register a user mapping.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_user_mapping(
    kv: &dyn Kv,
    user: &str,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    let ops = create_user_mapping_ops(kv, user, server, options)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for registering a user mapping without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves donor CRUD API ownership for metadata options"
)]
pub fn create_user_mapping_ops(
    kv: &dyn Kv,
    user: &str,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&key::user_mapping_key(user, server))?.is_some() {
        return Err(CatalogError::DuplicateObject(format!("{user}@{server}")));
    }
    Ok(vec![WriteOp::Put {
        key: key::user_mapping_key(user, server),
        value: serialize_user_mapping(user, server, &options),
    }])
}

/// Look up a user mapping.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_user_mapping(
    kv: &dyn Kv,
    user: &str,
    server: &str,
) -> Result<UserMapping, CatalogError> {
    let bytes = kv
        .get(&key::user_mapping_key(user, server))?
        .ok_or_else(|| CatalogError::UndefinedObject(format!("{user}@{server}")))?;
    Ok(deserialize_user_mapping(&bytes)?)
}

/// Drop a user mapping.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_user_mapping(kv: &dyn Kv, user: &str, server: &str) -> Result<(), CatalogError> {
    let ops = drop_user_mapping_ops(kv, user, server)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a user mapping without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_user_mapping_ops(
    kv: &dyn Kv,
    user: &str,
    server: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_user_mapping(kv, user, server)?;
    Ok(vec![WriteOp::Delete {
        key: key::user_mapping_key(user, server),
    }])
}

// ── Foreign table ─────────────────────────────────────────────────────────────

/// The envelope columns prepended to every foreign (Kafka) table.
fn envelope_columns() -> Vec<Column> {
    vec![
        Column::new("_partition", ColumnType::Int4),
        Column::new("_offset", ColumnType::Int8),
        Column::new("_timestamp", ColumnType::Timestamptz),
        Column::new("_key", ColumnType::Bytea),
        Column::new("_headers", ColumnType::Text),
    ]
}

/// Create a foreign table linked to an existing server.
///
/// The server must already exist (returns `UndefinedObject` otherwise).
/// Envelope columns are prepended; user-supplied value columns follow.
///
/// # Errors
///
/// Returns undefined-object, duplicate-table, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_foreign_table(
    kv: &dyn Kv,
    name: &str,
    value_columns: Vec<Column>,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_foreign_table_ops(kv, name, value_columns, server, options)?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Build the write batch for creating a foreign table without writing.
///
/// # Errors
///
/// Returns undefined-object, duplicate-table, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_foreign_table_ops(
    kv: &dyn Kv,
    name: &str,
    value_columns: Vec<Column>,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    let _ = get_server(kv, server)?;

    if relation_exists(kv, name)? {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }

    let next = read_next_table_id(kv)?;
    let mut columns = envelope_columns();
    columns.extend(value_columns);

    let meta = ForeignTableMeta {
        server: server.to_string(),
        options,
    };

    let batch = vec![
        WriteOp::Put {
            key: key::catalog_key(name),
            value: serialize_schema(next, &columns, TableOptions::default(), Some(&meta)),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: U64::new(1).as_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: U32::new(next + 1).as_bytes().to_vec(),
        },
    ];
    Ok((next, batch))
}

/// Read the next `TableId` (defaults to 1 when the meta key is absent).
fn read_next_table_id(kv: &dyn Kv) -> Result<TableId, CatalogError> {
    match kv.get(&key::meta_next_table_id_key())? {
        Some(b) => {
            let (v, _) = U32::read_from_prefix(b.as_slice())
                .map_err(|_| KvError::CorruptRow("next_table_id is not u32".into()))?;
            Ok(v.get())
        }
        None => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgkv::{FjallKv, MemKv};
    use crabka_pgtypes::ColumnType;

    use super::*;

    fn cols() -> Vec<Column> {
        vec![
            Column::new("id", ColumnType::Int4),
            Column::new("name", ColumnType::Text),
        ]
    }

    #[test]
    fn roles_and_table_privileges_round_trip() {
        let kv = MemKv::default();
        create_table(&kv, "docs", vec![Column::new("id", ColumnType::Int4)]).expect("table");
        create_role(&kv, "reader", false).expect("role");

        let ops = grant_table_privileges_ops(
            &kv,
            "docs",
            &["reader".to_string()],
            &["SELECT".to_string()],
        )
        .expect("grant ops");
        kv.write_batch(&ops).expect("grant write");

        assert_eq!(
            list_table_privileges(&kv).expect("privileges"),
            vec![TablePrivilege {
                table: "docs".into(),
                grantee: "reader".into(),
                privilege: "SELECT".into(),
            }]
        );

        let ops = revoke_table_privileges_ops(
            &kv,
            "docs",
            &["reader".to_string()],
            &["SELECT".to_string()],
        )
        .expect("revoke ops");
        kv.write_batch(&ops).expect("revoke write");
        assert!(list_table_privileges(&kv).expect("privileges").is_empty());
    }

    fn check_crud(kv: &dyn Kv) {
        let id = create_table(kv, "t", cols()).expect("create");
        let t = get_table(kv, "t").expect("lookup");
        assert_eq!(t.id, id);
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.column_index("id"), Some(0));
        assert_eq!(t.column_index("name"), Some(1));
        assert_eq!(t.column_index("nope"), None);
        assert!(t.foreign.is_none());
        assert!(!t.sharded);
        assert_eq!(
            create_table(kv, "t", cols()).expect_err("dup").sqlstate(),
            "42P07"
        );
        let id2 = create_table(kv, "u", cols()).expect("create u");
        assert_ne!(id, id2);
        drop_table(kv, "t").expect("drop");
        assert_eq!(get_table(kv, "t").expect_err("gone").sqlstate(), "42P01");
        assert_eq!(
            drop_table(kv, "nope").expect_err("missing").sqlstate(),
            "42P01"
        );
    }

    #[test]
    fn conversion_batch_rejects_metadata_only_rewrite() {
        let kv = MemKv::new();
        create_table(&kv, "conversion", cols()).expect("create table");

        assert_eq!(
            complete_table_conversion_ops(&kv, "conversion", None, Vec::new())
                .expect_err("empty rewrite must not publish conversion"),
            CatalogError::IncompleteConversionRewrite
        );
        assert!(
            !get_table(&kv, "conversion")
                .expect("table remains plain")
                .sharded
        );
    }

    #[test]
    fn views_persist_schema_share_relation_namespace_and_drop() {
        let kv = MemKv::new();
        let columns = vec![Column::new("total", ColumnType::Int4)];
        create_view(
            &kv,
            "sales_view",
            "SELECT 1 AS total".into(),
            columns.clone(),
        )
        .expect("create view");
        assert_eq!(
            get_view(&kv, "sales_view").expect("stored view"),
            View {
                name: "sales_view".into(),
                definition: "SELECT 1 AS total".into(),
                columns,
            }
        );
        assert_eq!(
            create_table(&kv, "sales_view", cols())
                .expect_err("view name owns relation namespace")
                .sqlstate(),
            "42P07"
        );
        assert_eq!(
            create_view(&kv, "sales_view", "SELECT 1".into(), vec![])
                .expect_err("duplicate view")
                .sqlstate(),
            "42P07"
        );
        drop_view(&kv, "sales_view").expect("drop view");
        assert_eq!(
            get_view(&kv, "sales_view")
                .expect_err("dropped view")
                .sqlstate(),
            "42P01"
        );

        create_table(&kv, "sales_table", cols()).expect("create table");
        assert_eq!(
            drop_view(&kv, "sales_table")
                .expect_err("table cannot be dropped as a view")
                .sqlstate(),
            "42809"
        );
    }

    #[test]
    fn conversion_batch_rejects_xid_tuple_reinserted_after_delete() {
        let kv = MemKv::new();
        let table_id = create_table(&kv, "conversion", cols()).expect("create table");
        let tuple_key = crabka_pgmvcc::version::version_key_xid(table_id, 1, 7);
        let xid_tuple = crabka_pgmvcc::version::encode_tuple(
            7,
            0,
            &[Datum::Int4(1), Datum::Text("old".into())],
        );
        kv.put(tuple_key.clone(), xid_tuple.clone())
            .expect("write old tuple");

        assert_eq!(
            complete_table_conversion_ops(
                &kv,
                "conversion",
                None,
                vec![
                    WriteOp::Delete {
                        key: tuple_key.clone(),
                    },
                    WriteOp::Put {
                        key: tuple_key,
                        value: xid_tuple,
                    },
                ],
            )
            .expect_err("final state contains an xid tuple"),
            CatalogError::IncompleteConversionRewrite
        );
    }

    fn check_fdw_crud(kv: &dyn Kv) {
        create_server(
            kv,
            "s",
            "kafka_fdw",
            vec![("bootstrap".into(), "h:9092".into())],
        )
        .expect("create server");
        let s = get_server(kv, "s").expect("get");
        assert_eq!(s.wrapper, "kafka_fdw");
        assert_eq!(
            create_server(kv, "s", "kafka_fdw", vec![])
                .expect_err("dup")
                .sqlstate(),
            "42710"
        );
        assert_eq!(
            get_server(kv, "nope").expect_err("missing").sqlstate(),
            "42704"
        );

        let cols = vec![Column::new("id", ColumnType::Int4)];
        create_foreign_table(
            kv,
            "orders",
            cols,
            "s",
            vec![("topic".into(), "orders".into())],
        )
        .expect("ft");
        let t = get_table(kv, "orders").expect("get ft");
        assert!(t.foreign.is_some());
        assert_eq!(t.columns[0].name, "_partition");
        assert_eq!(t.columns[0].ty, ColumnType::Int4);
        assert_eq!(t.columns[3].name, "_key");
        assert_eq!(t.columns.last().expect("value col").name, "id");
    }

    #[test]
    fn fdw_crud_memkv() {
        check_fdw_crud(&MemKv::new());
    }

    #[test]
    fn create_fdw_persists() {
        let kv = MemKv::new();
        create_fdw(&kv, "w", vec![]).expect("create");
        let fdw = get_fdw(&kv, "w").expect("must be persisted");
        assert_eq!(fdw.name, "w");
    }

    #[test]
    fn drop_fdw_removes() {
        let kv = MemKv::new();
        create_fdw(&kv, "w", vec![]).expect("create");
        drop_fdw(&kv, "w").expect("drop");
        assert_eq!(get_fdw(&kv, "w").expect_err("gone").sqlstate(), "42704");
    }

    #[test]
    fn drop_server_removes() {
        let kv = MemKv::new();
        create_server(&kv, "s", "fdw", vec![]).expect("create");
        drop_server(&kv, "s").expect("drop");
        assert_eq!(get_server(&kv, "s").expect_err("gone").sqlstate(), "42704");
    }

    #[test]
    fn create_user_mapping_persists() {
        let kv = MemKv::new();
        create_user_mapping(&kv, "alice", "s", vec![("k".into(), "v".into())]).expect("create");
        let m = get_user_mapping(&kv, "alice", "s").expect("must be persisted");
        assert_eq!(m.user, "alice");
        assert_eq!(m.server, "s");
    }

    #[test]
    fn drop_user_mapping_removes() {
        let kv = MemKv::new();
        create_user_mapping(&kv, "bob", "s2", vec![]).expect("create");
        drop_user_mapping(&kv, "bob", "s2").expect("drop");
        assert_eq!(
            get_user_mapping(&kv, "bob", "s2")
                .expect_err("gone")
                .sqlstate(),
            "42704"
        );
    }

    #[test]
    fn crud_on_memkv() {
        check_crud(&MemKv::new());
    }

    #[test]
    fn sharded_table_metadata_roundtrips() {
        let kv = MemKv::new();
        let id =
            create_table_with_options(&kv, "sharded_t", cols(), TableOptions { sharded: true })
                .expect("create sharded table");
        let table = get_table(&kv, "sharded_t").expect("lookup sharded table");
        assert_eq!(table.id, id);
        assert!(table.sharded);
        assert!(table.foreign.is_none());
    }

    #[test]
    fn table_hash_sharding_metadata_roundtrips() {
        let kv = MemKv::new();
        create_table_with_options(&kv, "hash_t", cols(), TableOptions { sharded: true })
            .expect("create hash table");
        let sharding = ShardingStrategy::Hash(HashSharding {
            columns: vec!["id".into()],
            buckets: 16,
            co_location_group: Some("group_a".into()),
        });

        kv.write_batch(
            &set_table_sharding_ops(&kv, "hash_t", Some(&sharding)).expect("sharding ops"),
        )
        .expect("write sharding");

        assert_eq!(
            get_table_sharding(&kv, "hash_t").expect("read sharding"),
            Some(sharding)
        );
    }

    #[test]
    fn create_index_metadata_roundtrips_and_lists_by_table() {
        let kv = MemKv::new();
        let table_id = create_table(&kv, "users", cols()).expect("create table");

        let index_id = create_index(
            &kv,
            "users_name_idx",
            "users",
            vec!["name".into()],
            true,
            IndexPlacement::Global,
        )
        .expect("create index");

        let expected = Index {
            id: index_id,
            name: "users_name_idx".into(),
            table: "users".into(),
            table_id,
            columns: vec!["name".into()],
            unique: true,
            placement: IndexPlacement::Global,
            constraint: None,
        };
        assert_eq!(get_index(&kv, "users_name_idx").expect("index"), expected);
        assert_eq!(
            list_table_indexes(&kv, "users").expect("list"),
            vec![expected]
        );
    }

    #[test]
    fn create_index_rejects_missing_columns_and_duplicate_names() {
        let kv = MemKv::new();
        create_table(&kv, "users", cols()).expect("create table");
        create_index(
            &kv,
            "users_name_idx",
            "users",
            vec!["name".into()],
            false,
            IndexPlacement::Local,
        )
        .expect("create index");

        assert_eq!(
            create_index(
                &kv,
                "users_name_idx",
                "users",
                vec!["id".into()],
                false,
                IndexPlacement::Local,
            )
            .expect_err("duplicate")
            .sqlstate(),
            "42P07"
        );
        assert_eq!(
            create_index(
                &kv,
                "users_bad_idx",
                "users",
                vec!["missing".into()],
                false,
                IndexPlacement::Local,
            )
            .expect_err("missing column")
            .sqlstate(),
            "42703"
        );
    }

    #[test]
    fn crud_on_fjallkv() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_crud(&FjallKv::open(dir.path()).expect("open"));
    }
}
