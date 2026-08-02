//! F-2: the `pg_catalog` and `information_schema` relations `psql`'s `\d`
//! family and ORM preambles read, beyond the starter set `exec` already owns.
//!
//! Two rules shape everything here:
//!
//! 1. **A named relation always exists.** Where crabka has no such object kind
//!    yet (triggers, policies, enums, publications), the relation still resolves
//!    and returns zero rows. A client that `LEFT JOIN`s it gets PostgreSQL's
//!    answer; one that expects rows gets an empty set rather than a 42P01.
//! 2. **Columns follow PostgreSQL 18.4's catalog order and names exactly**, so a
//!    positional `SELECT *` and a named projection agree with the oracle. Types
//!    are the nearest crabka [`ColumnType`]: `oid`/`xid` are integers, `name`,
//!    `"char"`, `pg_node_tree`, `int2vector`, `oidvector` and `pg_lsn` are
//!    `text`, and `aclitem[]` is a `text[]` that is always NULL (crabka grants
//!    are tracked outside the ACL representation).
//!
//! [`exec`](crate::exec) owns the relation seam; this module owns the extra
//! names, their column lists and their rows.

use std::collections::BTreeMap;

use crabka_pgcatalog::{
    Column, CommentObject, ForeignKeyId, IndexConstraint, MatchType, ReferentialAction,
    RelationName, Table,
};
use crabka_pgkv::Kv;
use crabka_pgtypes::{ColumnType, Datum, ElemType};

use crate::error::ExecError;

/// First oid of the band reserved for view relations.
const VIEW_OID_BASE: i32 = 60_000;
/// First oid of the band reserved for sequence relations.
const SEQUENCE_OID_BASE: i32 = 70_000;
/// First oid of the band reserved for user-created schemas (`pg_namespace`).
const NAMESPACE_OID_BASE: i32 = 140_000;
/// First oid of the band reserved for `CHECK` constraints.
const CHECK_OID_BASE: i32 = 90_000;
/// First oid of the band reserved for `NOT NULL` constraints, which PostgreSQL
/// 18 records in `pg_constraint` with `contype = 'n'`.
const NOT_NULL_OID_BASE: i32 = 130_000;
/// First oid of the band reserved for `FOREIGN KEY` constraints.
const FOREIGN_KEY_OID_BASE: i32 = 150_000;
/// First oid of the band reserved for column defaults (`pg_attrdef`).
const ATTRDEF_OID_BASE: i32 = 110_000;
/// Width of every name-hashed oid band.
const OID_BAND_WIDTH: i32 = 9_000;
/// Oid of the constraint band derived from an index oid.
const CONSTRAINT_OID_BASE: i32 = 80_000;
/// `pg_class` oid the index band starts at, mirroring `exec::catalog_index_oid`.
const INDEX_OID_BASE: i32 = 50_000;

/// Oid of the single database crabka exposes.
pub(crate) const DATABASE_OID: i32 = 5;
/// Oid of the `pg_default` tablespace, as in PostgreSQL.
const DEFAULT_TABLESPACE_OID: i32 = 1663;
/// Oid of the `btree` access method, as in PostgreSQL.
pub(crate) const BTREE_AM_OID: i32 = 403;
pub(crate) const HASH_AM_OID: i32 = 405;
pub(crate) const GIST_AM_OID: i32 = 783;
pub(crate) const GIN_AM_OID: i32 = 2742;
pub(crate) const SPGIST_AM_OID: i32 = 4000;
/// Oid of the `default` collation, as in PostgreSQL.
pub(crate) const DEFAULT_COLLATION_OID: i32 = 100;

/// Canonicalize a written relation name to this module's key, or `None` when
/// [`exec`](crate::exec) owns it (or nothing does).
pub(crate) fn catalog_relation(name: &str) -> Option<&'static str> {
    let bare = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if let Some(found) = PG_CATALOG_RELATIONS.iter().find(|entry| **entry == bare) {
        return Some(found);
    }
    let qualified = name.strip_prefix("information_schema.")?;
    INFORMATION_SCHEMA_RELATIONS
        .iter()
        .find(|entry| entry.strip_prefix("information_schema.") == Some(qualified))
        .copied()
}

/// Every relation this module owns, in the order `pg_class` reports them.
pub(crate) fn relation_names() -> &'static [&'static str] {
    RELATION_NAMES
}

const PG_CATALOG_RELATIONS: &[&str] = &[
    "pg_am",
    "pg_attrdef",
    "pg_authid",
    "pg_collation",
    "pg_constraint",
    "pg_database",
    "pg_depend",
    "pg_description",
    "pg_enum",
    "pg_event_trigger",
    "pg_extension",
    "pg_indexes",
    "pg_inherits",
    "pg_language",
    "pg_locks",
    "pg_partitioned_table",
    "pg_policy",
    "pg_proc",
    "pg_publication",
    "pg_publication_namespace",
    "pg_publication_rel",
    "pg_replication_slots",
    "pg_rewrite",
    "pg_sequence",
    "pg_shdescription",
    "pg_statistic_ext",
    "pg_stat_activity",
    "pg_tables",
    "pg_tablespace",
    "pg_trigger",
    "pg_views",
];

const INFORMATION_SCHEMA_RELATIONS: &[&str] = &[
    "information_schema.applicable_roles",
    "information_schema.column_privileges",
    "information_schema.constraint_column_usage",
    "information_schema.enabled_roles",
    "information_schema.key_column_usage",
    "information_schema.parameters",
    "information_schema.referential_constraints",
    "information_schema.routines",
    "information_schema.sequences",
    "information_schema.table_constraints",
    "information_schema.table_privileges",
    "information_schema.views",
];

static RELATION_NAMES: &[&str] = &[
    "pg_am",
    "pg_attrdef",
    "pg_authid",
    "pg_collation",
    "pg_constraint",
    "pg_database",
    "pg_depend",
    "pg_description",
    "pg_enum",
    "pg_event_trigger",
    "pg_extension",
    "pg_indexes",
    "pg_inherits",
    "pg_language",
    "pg_locks",
    "pg_partitioned_table",
    "pg_policy",
    "pg_proc",
    "pg_publication",
    "pg_publication_namespace",
    "pg_publication_rel",
    "pg_replication_slots",
    "pg_rewrite",
    "pg_sequence",
    "pg_shdescription",
    "pg_statistic_ext",
    "pg_stat_activity",
    "pg_tables",
    "pg_tablespace",
    "pg_trigger",
    "pg_views",
    "information_schema.applicable_roles",
    "information_schema.column_privileges",
    "information_schema.constraint_column_usage",
    "information_schema.enabled_roles",
    "information_schema.key_column_usage",
    "information_schema.parameters",
    "information_schema.referential_constraints",
    "information_schema.routines",
    "information_schema.sequences",
    "information_schema.table_constraints",
    "information_schema.table_privileges",
    "information_schema.views",
];

/// The fixed `pg_class` oid of one of this module's relations. PostgreSQL's own
/// oid where the relation is a real catalog table; a reserved 12xxxx value where
/// it is a system view whose oid nothing depends on.
pub(crate) fn relation_oid(name: &str) -> i32 {
    match name {
        "pg_am" => 2601,
        "pg_attrdef" => 2604,
        "pg_authid" => 1260,
        "pg_collation" => 3456,
        "pg_constraint" => 2606,
        "pg_database" => 1262,
        "pg_depend" => 2608,
        "pg_description" => 2609,
        "pg_enum" => 3501,
        "pg_event_trigger" => 3466,
        "pg_extension" => 3079,
        "pg_inherits" => 2611,
        "pg_language" => 2612,
        "pg_partitioned_table" => 3350,
        "pg_policy" => 3256,
        "pg_proc" => 1255,
        "pg_publication" => 6104,
        "pg_publication_namespace" => 6237,
        "pg_publication_rel" => 6106,
        "pg_rewrite" => 2618,
        "pg_sequence" => 2224,
        "pg_shdescription" => 2396,
        "pg_statistic_ext" => 3381,
        "pg_tablespace" => 1213,
        "pg_trigger" => 2620,
        _ => system_view_oid(name),
    }
}

/// Reserved oids for the relations above that are views rather than catalog
/// tables. Kept out of [`relation_oid`]'s match so neither arm grows unwieldy.
fn system_view_oid(name: &str) -> i32 {
    match name {
        "pg_indexes" => 120_001,
        "pg_locks" => 120_002,
        "pg_replication_slots" => 120_003,
        "pg_stat_activity" => 120_004,
        "pg_tables" => 120_005,
        "pg_views" => 120_006,
        "information_schema.applicable_roles" => 120_010,
        "information_schema.column_privileges" => 120_011,
        "information_schema.constraint_column_usage" => 120_012,
        "information_schema.enabled_roles" => 120_013,
        "information_schema.key_column_usage" => 120_014,
        "information_schema.parameters" => 120_015,
        "information_schema.referential_constraints" => 120_016,
        "information_schema.routines" => 120_017,
        "information_schema.sequences" => 120_018,
        "information_schema.table_constraints" => 120_019,
        "information_schema.table_privileges" => 120_020,
        "information_schema.views" => 120_021,
        _ => 0,
    }
}

/// What a banded oid is hashed from.
///
/// A relation is keyed by its own [`RelationName`], so two same-named relations
/// in different schemas take different oids — the hash has to see the schema,
/// which the `public`-bare [`std::fmt::Display`] spelling would hide. Everything
/// else arrives as an already-built dotted key.
trait BandKey: Ord + Clone {
    fn band_text(&self) -> String;
}

impl BandKey for String {
    fn band_text(&self) -> String {
        self.clone()
    }
}

impl BandKey for RelationName {
    fn band_text(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// What a `pg_constraint` oid is keyed by: the relation the constraint belongs
/// to, and the constraint's own name — the column's name, for the `NOT NULL`
/// constraints `PostgreSQL` 18 records per column.
///
/// Structural rather than a flattened `<table>.<name>` string, for the same
/// reason [`BandKey for RelationName`](BandKey) spells the schema out. A
/// constraint name is unique per relation rather than per catalog, so the key
/// has to carry the relation; and a dot inside a quoted identifier — `"s.t"` in
/// `public` against `t` in schema `s` — would let two distinct constraints
/// flatten to one key. [`banded_oids`] deduplicates, so that is not a near miss
/// in the band: the pair becomes one entry and reports one oid twice.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ConstraintKey {
    table: RelationName,
    name: String,
}

impl ConstraintKey {
    pub(crate) fn new(table: &RelationName, name: &str) -> Self {
        Self {
            table: table.clone(),
            name: name.to_string(),
        }
    }
}

impl BandKey for ConstraintKey {
    fn band_text(&self) -> String {
        format!("{}.{}", self.table.band_text(), self.name)
    }
}

/// Assign every name in `names` a distinct oid inside the band starting at
/// `base`. The slot is a hash of the name, so an object keeps its oid when
/// unrelated objects are created or dropped; a collision probes forward, which
/// makes the whole assignment a pure function of the (sorted) name set.
fn banded_oids<K: BandKey>(base: i32, names: &[K]) -> BTreeMap<K, i32> {
    let mut taken = BTreeMap::new();
    let mut used = std::collections::BTreeSet::new();
    let mut sorted: Vec<&K> = names.iter().collect();
    sorted.sort();
    sorted.dedup();
    for name in sorted {
        let mut slot =
            i32::try_from(fnv1a(&name.band_text()) % OID_BAND_WIDTH.unsigned_abs()).unwrap_or(0);
        while !used.insert(slot) {
            slot = (slot + 1) % OID_BAND_WIDTH;
        }
        taken.insert(name.clone(), base + slot);
    }
    taken
}

/// FNV-1a over the name's bytes — a stable, dependency-free 32-bit fold.
fn fnv1a(value: &str) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in value.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// `pg_namespace.oid` of a schema by name.
///
/// The three schemas `PostgreSQL` itself bootstraps keep their real oids, so a
/// client that hard-codes 2200 for `public` still matches; a user-created
/// schema gets a stable hashed slot in its own reserved band, exactly as views
/// and sequences do.
pub(crate) fn namespace_oid(schema: &str) -> i32 {
    match schema {
        "public" => crate::exec::PUBLIC_NAMESPACE_OID,
        "pg_catalog" => crate::exec::PG_CATALOG_NAMESPACE_OID,
        "information_schema" => crate::exec::INFORMATION_SCHEMA_NAMESPACE_OID,
        other => {
            let slot =
                i32::try_from(fnv1a(other) % OID_BAND_WIDTH.unsigned_abs()).unwrap_or_default();
            NAMESPACE_OID_BASE + slot
        }
    }
}

/// `pg_class` oids of every view, keyed by the view's catalog name.
pub(crate) fn view_oids(kv: &dyn Kv) -> Result<BTreeMap<RelationName, i32>, ExecError> {
    let names = crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| view.name)
        .collect::<Vec<_>>();
    Ok(banded_oids(VIEW_OID_BASE, &names))
}

/// `pg_class` oids of every sequence, keyed by the sequence's catalog name.
pub(crate) fn sequence_oids(kv: &dyn Kv) -> Result<BTreeMap<RelationName, i32>, ExecError> {
    let names = crabka_pgcatalog::list_sequences(kv)?
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    Ok(banded_oids(SEQUENCE_OID_BASE, &names))
}

/// Role oids, keyed by role name. The bootstrap superuser keeps PostgreSQL's
/// own oid 10; every other role gets a hashed slot.
pub(crate) fn role_oids(kv: &dyn Kv) -> Result<BTreeMap<String, i32>, ExecError> {
    let names = crabka_pgcatalog::list_roles(kv)?
        .into_iter()
        .map(|role| role.name)
        .filter(|name| name != crate::catalog_fn::OBJECT_OWNER)
        .collect::<Vec<_>>();
    let mut oids = banded_oids(crate::catalog_fn::ROLE_OID_BASE, &names);
    oids.insert(
        crate::catalog_fn::OBJECT_OWNER.to_string(),
        crate::catalog_fn::BOOTSTRAP_ROLE_OID,
    );
    Ok(oids)
}

/// `pg_constraint` oids of every `CHECK` constraint, keyed by its relation and
/// name.
pub(crate) fn check_constraint_oids(
    kv: &dyn Kv,
) -> Result<BTreeMap<ConstraintKey, i32>, ExecError> {
    let keys = crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .flat_map(|table| {
            table
                .checks
                .iter()
                .map(|check| ConstraintKey::new(&table.name, &check.name))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(banded_oids(CHECK_OID_BASE, &keys))
}

/// `pg_constraint` oids of every `NOT NULL` constraint, keyed by its relation
/// and the column it constrains.
pub(crate) fn not_null_constraint_oids(
    kv: &dyn Kv,
) -> Result<BTreeMap<ConstraintKey, i32>, ExecError> {
    let keys = crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .flat_map(|table| {
            table
                .columns
                .iter()
                .filter(|column| column.not_null)
                .map(|column| ConstraintKey::new(&table.name, &column.name))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(banded_oids(NOT_NULL_OID_BASE, &keys))
}

/// Every `FOREIGN KEY` constraint's oid, indexed by the `<child table>.<name>`
/// text `pg_get_constraintdef` reverses an oid through.
///
/// The oid itself comes from [`foreign_key_oid`]; this map is a lookup index
/// over it, not the definition of it.
pub(crate) fn foreign_key_constraint_oids(kv: &dyn Kv) -> Result<BTreeMap<String, i32>, ExecError> {
    crabka_pgcatalog::list_foreign_keys(kv)?
        .into_iter()
        .map(|foreign_key| {
            Ok((
                format!("{}.{}", foreign_key.table, foreign_key.name),
                foreign_key_oid(foreign_key.id)?,
            ))
        })
        .collect()
}

/// The `pg_constraint` oid of a foreign key: its stored [`ForeignKeyId`] placed
/// in the foreign-key band, exactly as [`index_constraint_oid`] places an index
/// id in its own.
///
/// The id is monotonic, drawn from the catalog's foreign-key counter, and
/// survives a rename, so the oid it yields is stable for the constraint's
/// lifetime and orders by creation — both properties a real `pg_constraint.oid`
/// has and a slot hashed from the constraint's *name* does not.
pub(crate) fn foreign_key_oid(id: ForeignKeyId) -> Result<i32, ExecError> {
    i32::try_from(id)
        .ok()
        .filter(|id| *id < OID_BAND_WIDTH)
        .and_then(|id| FOREIGN_KEY_OID_BASE.checked_add(id))
        .ok_or_else(|| ExecError::Unsupported("foreign key oid leaves its band".into()))
}

/// The `pg_constraint` oid of the constraint an index backs.
///
/// Bounded to the band like [`foreign_key_oid`]: the bases sit
/// [`OID_BAND_WIDTH`] apart, so an unbounded add would walk an index id past
/// `CONSTRAINT_OID_BASE + OID_BAND_WIDTH` into the `CHECK` band and hand two
/// distinct constraints one oid. Refusing is the lesser failure — a wrong oid
/// is silent, and `pg_constraint` is what a client joins on.
pub(crate) fn index_constraint_oid(index_id: u32) -> Result<i32, ExecError> {
    i32::try_from(index_id)
        .ok()
        .filter(|id| *id < OID_BAND_WIDTH)
        .and_then(|id| CONSTRAINT_OID_BASE.checked_add(id))
        .ok_or_else(|| ExecError::Unsupported("constraint oid leaves its band".into()))
}

/// The `pg_class` oid of an index, mirroring `exec::catalog_index_oid` so both
/// sides of a `pg_index`/`pg_class` join agree.
///
/// Bounded for the same reason as [`index_constraint_oid`]; the neighbour it
/// would spill into is the view band.
pub(crate) fn index_relation_oid(index_id: u32) -> Result<i32, ExecError> {
    i32::try_from(index_id)
        .ok()
        .filter(|id| *id < OID_BAND_WIDTH)
        .and_then(|id| INDEX_OID_BASE.checked_add(id))
        .ok_or_else(|| ExecError::Unsupported("index oid leaves its band".into()))
}

/// The `pg_class` oid of a table, which is its catalog id.
fn table_relation_oid(table_id: u32) -> Result<i32, ExecError> {
    i32::try_from(table_id).map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))
}

/// Build a column list from `(name, type)` pairs.
fn cols(defs: &[(&str, ColumnType)]) -> Vec<Column> {
    defs.iter()
        .map(|(name, ty)| Column::new(*name, *ty))
        .collect()
}

fn text(value: &str) -> Datum {
    Datum::Text(value.to_string())
}

fn int(value: i32) -> Datum {
    Datum::Int4(value)
}

fn small(value: i16) -> Datum {
    Datum::Int2(value)
}

/// The relation's column list, or an empty list for a name this module does not
/// own (the seam never asks for one).
pub(crate) fn columns(name: &str) -> Vec<Column> {
    let found = pg_catalog_columns(name);
    if found.is_empty() {
        information_schema_columns(name)
    } else {
        found
    }
}

/// The relation's rows. `backend_pid` is the querying session's backend id,
/// which `pg_stat_activity` reports as its one row's `pid`.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn rows(
    kv: &dyn Kv,
    name: &str,
    backend_pid: i32,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    match name {
        "pg_am" => Ok(pg_am_rows()),
        "pg_language" => Ok(pg_language_rows()),
        "pg_proc" => crate::routine::pg_proc_rows(kv),
        "pg_attrdef" => pg_attrdef_rows(kv),
        "pg_authid" => pg_authid_rows(kv),
        "pg_collation" => Ok(pg_collation_rows()),
        "pg_constraint" => pg_constraint_rows(kv),
        "pg_database" => Ok(pg_database_rows()),
        "pg_depend" => pg_depend_rows(kv),
        "pg_description" => pg_description_rows(kv),
        "pg_event_trigger" => pg_event_trigger_rows(kv),
        "pg_indexes" => pg_indexes_rows(kv),
        "pg_rewrite" => pg_rewrite_rows(kv),
        "pg_sequence" => pg_sequence_rows(kv),
        "pg_stat_activity" => Ok(pg_stat_activity_rows(backend_pid)),
        "pg_tables" => pg_tables_rows(kv),
        "pg_tablespace" => Ok(pg_tablespace_rows()),
        "pg_trigger" => pg_trigger_rows(kv),
        "pg_views" => pg_views_rows(kv),
        _ => information_schema_rows(kv, name),
    }
}

fn pg_trigger_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgcatalog::trigger::{TriggerLevel, TriggerTiming};
    let mut tables: std::collections::HashMap<_, _> = crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .map(|table| (table.id, table))
        .collect();
    for view in crabka_pgcatalog::list_views(kv)? {
        let table = crate::trigger::relation_trigger_table(kv, &view.name)?;
        tables.insert(table.id, table);
    }
    crabka_pgcatalog::trigger::list_triggers(kv)?
        .into_iter()
        .map(|trigger| {
            let mut ty = match trigger.level {
                TriggerLevel::Row => 1,
                TriggerLevel::Statement => 0,
            };
            ty |= match trigger.timing {
                TriggerTiming::Before => 2,
                TriggerTiming::After => 0,
                TriggerTiming::InsteadOf => 64,
            };
            ty |= i16::from(trigger.events.insert) * 4;
            ty |= i16::from(trigger.events.delete) * 8;
            ty |= i16::from(trigger.events.update) * 16;
            ty |= i16::from(trigger.events.truncate) * 32;
            let attrs = tables
                .get(&trigger.table_id)
                .map_or_else(String::new, |table| {
                    trigger
                        .events
                        .update_columns
                        .iter()
                        .filter_map(|name| table.column_index(name).map(|index| index + 1))
                        .map(|number| number.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                });
            let mut args = Vec::new();
            for argument in &trigger.arguments {
                args.extend_from_slice(argument.as_bytes());
                args.push(0);
            }
            Ok(vec![
                int(i32::try_from(trigger.oid).unwrap_or(0)),
                int(i32::try_from(trigger.table_id).unwrap_or(0)),
                int(i32::try_from(trigger.parent_oid).unwrap_or(0)),
                text(&trigger.name),
                int(i32::try_from(trigger.function_oid).unwrap_or(0)),
                small(ty),
                text(&trigger.enabled.catalog_code().to_string()),
                Datum::Bool(trigger.is_internal),
                int(i32::try_from(trigger.referenced_table_id.unwrap_or(0)).unwrap_or(0)),
                int(0),
                int(i32::try_from(trigger.constraint_oid).unwrap_or(0)),
                Datum::Bool(trigger.deferrable),
                Datum::Bool(trigger.initially_deferred),
                small(i16::try_from(trigger.arguments.len()).unwrap_or(i16::MAX)),
                text(&attrs),
                Datum::Bytea(args),
                trigger.when.as_deref().map_or(Datum::Null, text),
                trigger.old_transition.as_deref().map_or(Datum::Null, text),
                trigger.new_transition.as_deref().map_or(Datum::Null, text),
            ])
        })
        .collect()
}

fn pg_depend_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let trigger_class = relation_oid("pg_trigger");
    let event_trigger_class = relation_oid("pg_event_trigger");
    let proc_class = relation_oid("pg_proc");
    let relation_class = relation_oid("pg_class");
    let mut rows = Vec::new();
    for trigger in crabka_pgcatalog::trigger::list_triggers(kv)? {
        let oid = i32::try_from(trigger.oid).unwrap_or(0);
        rows.push(vec![
            int(trigger_class),
            int(oid),
            int(0),
            int(proc_class),
            int(i32::try_from(trigger.function_oid).unwrap_or(0)),
            int(0),
            text("n"),
        ]);
        rows.push(vec![
            int(trigger_class),
            int(oid),
            int(0),
            int(relation_class),
            int(i32::try_from(trigger.table_id).unwrap_or(0)),
            int(0),
            text("a"),
        ]);
    }
    for trigger in crabka_pgcatalog::trigger::list_event_triggers(kv)? {
        rows.push(vec![
            int(event_trigger_class),
            int(i32::try_from(trigger.oid).unwrap_or(0)),
            int(0),
            int(proc_class),
            int(i32::try_from(trigger.function_oid).unwrap_or(0)),
            int(0),
            text("n"),
        ]);
    }
    Ok(rows)
}

fn pg_event_trigger_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgcatalog::trigger::EventTriggerEvent;
    let role_oids = role_oids(kv)?;
    Ok(crabka_pgcatalog::trigger::list_event_triggers(kv)?
        .into_iter()
        .map(|trigger| {
            let event = match trigger.event {
                EventTriggerEvent::Login => "login",
                EventTriggerEvent::DdlCommandStart => "ddl_command_start",
                EventTriggerEvent::DdlCommandEnd => "ddl_command_end",
                EventTriggerEvent::SqlDrop => "sql_drop",
                EventTriggerEvent::TableRewrite => "table_rewrite",
            };
            let tags = trigger
                .filters
                .iter()
                .filter(|filter| filter.variable == "tag")
                .flat_map(|filter| filter.values.iter().cloned())
                .map(Datum::Text)
                .collect::<Vec<_>>();
            vec![
                int(i32::try_from(trigger.oid).unwrap_or(0)),
                text(&trigger.name),
                text(event),
                int(role_oids.get(&trigger.owner).copied().unwrap_or(0)),
                int(i32::try_from(trigger.function_oid).unwrap_or(0)),
                text(&trigger.enabled.catalog_code().to_string()),
                if tags.is_empty() {
                    Datum::Null
                } else {
                    Datum::Array(crabka_pgtypes::ArrayValue::new(ElemType::Text, tags))
                },
            ]
        })
        .collect())
}

// ---------------------------------------------------------------- pg_catalog

/// Column lists for the `pg_catalog` relations, in PostgreSQL 18.4 order.
fn pg_catalog_columns(name: &str) -> Vec<Column> {
    use ColumnType::{Bool, Float4, Int2, Int4, Int8, Text, Timestamptz};
    let acl = ColumnType::Array(ElemType::Text);
    match name {
        "pg_am" => cols(&[
            ("oid", Int4),
            ("amname", Text),
            ("amhandler", Text),
            ("amtype", Text),
        ]),
        "pg_attrdef" => cols(&[
            ("oid", Int4),
            ("adrelid", Int4),
            ("adnum", Int2),
            ("adbin", Text),
        ]),
        "pg_authid" => cols(&[
            ("oid", Int4),
            ("rolname", Text),
            ("rolsuper", Bool),
            ("rolinherit", Bool),
            ("rolcreaterole", Bool),
            ("rolcreatedb", Bool),
            ("rolcanlogin", Bool),
            ("rolreplication", Bool),
            ("rolbypassrls", Bool),
            ("rolconnlimit", Int4),
            ("rolpassword", Text),
            ("rolvaliduntil", Timestamptz),
        ]),
        "pg_collation" => cols(&[
            ("oid", Int4),
            ("collname", Text),
            ("collnamespace", Int4),
            ("collowner", Int4),
            ("collprovider", Text),
            ("collisdeterministic", Bool),
            ("collencoding", Int4),
            ("collcollate", Text),
            ("collctype", Text),
            ("colllocale", Text),
            ("collicurules", Text),
            ("collversion", Text),
        ]),
        "pg_constraint" => pg_constraint_columns(),
        "pg_database" => pg_database_columns(),
        "pg_depend" => cols(&[
            ("classid", Int4),
            ("objid", Int4),
            ("objsubid", Int4),
            ("refclassid", Int4),
            ("refobjid", Int4),
            ("refobjsubid", Int4),
            ("deptype", Text),
        ]),
        "pg_description" => cols(&[
            ("objoid", Int4),
            ("classoid", Int4),
            ("objsubid", Int4),
            ("description", Text),
        ]),
        "pg_shdescription" => cols(&[("objoid", Int4), ("classoid", Int4), ("description", Text)]),
        "pg_extension" => cols(&[
            ("oid", Int4),
            ("extname", Text),
            ("extowner", Int4),
            ("extnamespace", Int4),
            ("extrelocatable", Bool),
            ("extversion", Text),
            ("extconfig", ColumnType::Array(ElemType::Int4)),
            ("extcondition", ColumnType::Array(ElemType::Text)),
        ]),
        "pg_enum" => cols(&[
            ("oid", Int4),
            ("enumtypid", Int4),
            ("enumsortorder", Float4),
            ("enumlabel", Text),
        ]),
        "pg_inherits" => cols(&[
            ("inhrelid", Int4),
            ("inhparent", Int4),
            ("inhseqno", Int4),
            ("inhdetachpending", Bool),
        ]),
        "pg_partitioned_table" => cols(&[
            ("partrelid", Int4),
            ("partstrat", Text),
            ("partnatts", Int2),
            ("partdefid", Int4),
            ("partattrs", Text),
            ("partclass", Text),
            ("partcollation", Text),
            ("partexprs", Text),
        ]),
        "pg_proc" => pg_proc_columns(),
        "pg_rewrite" => cols(&[
            ("oid", Int4),
            ("rulename", Text),
            ("ev_class", Int4),
            ("ev_type", Text),
            ("ev_enabled", Text),
            ("is_instead", Bool),
            ("ev_qual", Text),
            ("ev_action", Text),
        ]),
        "pg_sequence" => cols(&[
            ("seqrelid", Int4),
            ("seqtypid", Int4),
            ("seqstart", Int8),
            ("seqincrement", Int8),
            ("seqmax", Int8),
            ("seqmin", Int8),
            ("seqcache", Int8),
            ("seqcycle", Bool),
        ]),
        "pg_tablespace" => cols(&[
            ("oid", Int4),
            ("spcname", Text),
            ("spcowner", Int4),
            ("spcacl", acl),
            ("spcoptions", ColumnType::Array(ElemType::Text)),
        ]),
        _ => pg_catalog_columns_rest(name),
    }
}

/// The remainder of the `pg_catalog` column lists — split out so neither arm of
/// the dispatch exceeds the file's function-length budget.
fn pg_catalog_columns_rest(name: &str) -> Vec<Column> {
    use ColumnType::{Bool, Int2, Int4, Int8, Text, Timestamptz};
    match name {
        "pg_trigger" => cols(&[
            ("oid", Int4),
            ("tgrelid", Int4),
            ("tgparentid", Int4),
            ("tgname", Text),
            ("tgfoid", Int4),
            ("tgtype", Int2),
            ("tgenabled", Text),
            ("tgisinternal", Bool),
            ("tgconstrrelid", Int4),
            ("tgconstrindid", Int4),
            ("tgconstraint", Int4),
            ("tgdeferrable", Bool),
            ("tginitdeferred", Bool),
            ("tgnargs", Int2),
            ("tgattr", Text),
            ("tgargs", ColumnType::Bytea),
            ("tgqual", Text),
            ("tgoldtable", Text),
            ("tgnewtable", Text),
        ]),
        "pg_event_trigger" => cols(&[
            ("oid", Int4),
            ("evtname", Text),
            ("evtevent", Text),
            ("evtowner", Int4),
            ("evtfoid", Int4),
            ("evtenabled", Text),
            ("evttags", ColumnType::Array(ElemType::Text)),
        ]),
        "pg_language" => cols(&[
            ("oid", Int4),
            ("lanname", Text),
            ("lanowner", Int4),
            ("lanispl", Bool),
            ("lanpltrusted", Bool),
            ("lanplcallfoid", Int4),
            ("laninline", Int4),
            ("lanvalidator", Int4),
            ("lanacl", ColumnType::Array(ElemType::Text)),
        ]),
        "pg_policy" => cols(&[
            ("oid", Int4),
            ("polname", Text),
            ("polrelid", Int4),
            ("polcmd", Text),
            ("polpermissive", Bool),
            ("polroles", ColumnType::Array(ElemType::Int4)),
            ("polqual", Text),
            ("polwithcheck", Text),
        ]),
        "pg_statistic_ext" => cols(&[
            ("oid", Int4),
            ("stxrelid", Int4),
            ("stxname", Text),
            ("stxnamespace", Int4),
            ("stxowner", Int4),
            ("stxstattarget", Int2),
            ("stxkeys", Text),
            ("stxkind", ColumnType::Array(ElemType::Text)),
        ]),
        "pg_publication" => cols(&[
            ("oid", Int4),
            ("pubname", Text),
            ("pubowner", Int4),
            ("puballtables", Bool),
            ("pubinsert", Bool),
            ("pubupdate", Bool),
            ("pubdelete", Bool),
            ("pubtruncate", Bool),
            ("pubviaroot", Bool),
        ]),
        "pg_publication_rel" => cols(&[
            ("oid", Int4),
            ("prpubid", Int4),
            ("prrelid", Int4),
            ("prqual", Text),
            ("prattrs", Text),
        ]),
        "pg_publication_namespace" => cols(&[("oid", Int4), ("pnpubid", Int4), ("pnnspid", Int4)]),
        "pg_locks" => cols(&[
            ("locktype", Text),
            ("database", Int4),
            ("relation", Int4),
            ("page", Int4),
            ("tuple", Int2),
            ("virtualxid", Text),
            ("transactionid", Int8),
            ("classid", Int4),
            ("objid", Int4),
            ("objsubid", Int2),
            ("virtualtransaction", Text),
            ("pid", Int4),
            ("mode", Text),
            ("granted", Bool),
            ("fastpath", Bool),
            ("waitstart", Timestamptz),
        ]),
        "pg_replication_slots" => pg_replication_slots_columns(),
        "pg_stat_activity" => pg_stat_activity_columns(),
        "pg_indexes" => cols(&[
            ("schemaname", Text),
            ("tablename", Text),
            ("indexname", Text),
            ("tablespace", Text),
            ("indexdef", Text),
        ]),
        "pg_tables" => cols(&[
            ("schemaname", Text),
            ("tablename", Text),
            ("tableowner", Text),
            ("tablespace", Text),
            ("hasindexes", Bool),
            ("hasrules", Bool),
            ("hastriggers", Bool),
            ("rowsecurity", Bool),
        ]),
        "pg_views" => cols(&[
            ("schemaname", Text),
            ("viewname", Text),
            ("viewowner", Text),
            ("definition", Text),
        ]),
        _ => Vec::new(),
    }
}

fn pg_constraint_columns() -> Vec<Column> {
    use ColumnType::{Bool, Int2, Int4, Text};
    let int2s = ColumnType::Array(ElemType::Int2);
    let oids = ColumnType::Array(ElemType::Int4);
    cols(&[
        ("oid", Int4),
        ("conname", Text),
        ("connamespace", Int4),
        ("contype", Text),
        ("condeferrable", Bool),
        ("condeferred", Bool),
        ("conenforced", Bool),
        ("convalidated", Bool),
        ("conrelid", Int4),
        ("contypid", Int4),
        ("conindid", Int4),
        ("conparentid", Int4),
        ("confrelid", Int4),
        ("confupdtype", Text),
        ("confdeltype", Text),
        ("confmatchtype", Text),
        ("conislocal", Bool),
        ("coninhcount", Int2),
        ("connoinherit", Bool),
        ("conperiod", Bool),
        ("conkey", int2s),
        ("confkey", int2s),
        ("conpfeqop", oids),
        ("conppeqop", oids),
        ("conffeqop", oids),
        ("confdelsetcols", int2s),
        ("conexclop", oids),
        ("conbin", Text),
    ])
}

fn pg_database_columns() -> Vec<Column> {
    use ColumnType::{Bool, Int4, Int8, Text};
    cols(&[
        ("oid", Int4),
        ("datname", Text),
        ("datdba", Int4),
        ("encoding", Int4),
        ("datlocprovider", Text),
        ("datistemplate", Bool),
        ("datallowconn", Bool),
        ("dathasloginevt", Bool),
        ("datconnlimit", Int4),
        ("datfrozenxid", Int8),
        ("datminmxid", Int8),
        ("dattablespace", Int4),
        ("datcollate", Text),
        ("datctype", Text),
        ("datlocale", Text),
        ("daticurules", Text),
        ("datcollversion", Text),
        ("datacl", ColumnType::Array(ElemType::Text)),
    ])
}

fn pg_proc_columns() -> Vec<Column> {
    use ColumnType::{Bool, Float4, Int2, Int4, Text};
    let texts = ColumnType::Array(ElemType::Text);
    let oids = ColumnType::Array(ElemType::Int4);
    cols(&[
        ("oid", Int4),
        ("proname", Text),
        ("pronamespace", Int4),
        ("proowner", Int4),
        ("prolang", Int4),
        ("procost", Float4),
        ("prorows", Float4),
        ("provariadic", Int4),
        ("prosupport", Text),
        ("prokind", Text),
        ("prosecdef", Bool),
        ("proleakproof", Bool),
        ("proisstrict", Bool),
        ("proretset", Bool),
        ("provolatile", Text),
        ("proparallel", Text),
        ("pronargs", Int2),
        ("pronargdefaults", Int2),
        ("prorettype", Int4),
        ("proargtypes", Text),
        ("proallargtypes", oids),
        ("proargmodes", ColumnType::Array(ElemType::Text)),
        ("proargnames", texts),
        ("proargdefaults", Text),
        ("protrftypes", ColumnType::Array(ElemType::Int4)),
        ("prosrc", Text),
        ("probin", Text),
        ("prosqlbody", Text),
        ("proconfig", ColumnType::Array(ElemType::Text)),
        ("proacl", ColumnType::Array(ElemType::Text)),
    ])
}

fn pg_replication_slots_columns() -> Vec<Column> {
    use ColumnType::{Bool, Int4, Int8, Text, Timestamptz};
    cols(&[
        ("slot_name", Text),
        ("plugin", Text),
        ("slot_type", Text),
        ("datoid", Int4),
        ("database", Text),
        ("temporary", Bool),
        ("active", Bool),
        ("active_pid", Int4),
        ("xmin", Int8),
        ("catalog_xmin", Int8),
        ("restart_lsn", Text),
        ("confirmed_flush_lsn", Text),
        ("wal_status", Text),
        ("safe_wal_size", Int8),
        ("two_phase", Bool),
        ("two_phase_at", Text),
        ("inactive_since", Timestamptz),
        ("conflicting", Bool),
        ("invalidation_reason", Text),
        ("failover", Bool),
        ("synced", Bool),
    ])
}

fn pg_stat_activity_columns() -> Vec<Column> {
    use ColumnType::{Int4, Int8, Text, Timestamptz};
    cols(&[
        ("datid", Int4),
        ("datname", Text),
        ("pid", Int4),
        ("leader_pid", Int4),
        ("usesysid", Int4),
        ("usename", Text),
        ("application_name", Text),
        ("client_addr", Text),
        ("client_hostname", Text),
        ("client_port", Int4),
        ("backend_start", Timestamptz),
        ("xact_start", Timestamptz),
        ("query_start", Timestamptz),
        ("state_change", Timestamptz),
        ("wait_event_type", Text),
        ("wait_event", Text),
        ("state", Text),
        ("backend_xid", Int8),
        ("backend_xmin", Int8),
        ("query_id", Int8),
        ("query", Text),
        ("backend_type", Text),
    ])
}

/// PostgreSQL's built-in index access methods, with their real oids: `relam`
/// joins against this and `\d`'s "Access method" line reads `amname`.
fn pg_am_rows() -> Vec<Vec<Datum>> {
    [
        (BTREE_AM_OID, "btree", "i"),
        (HASH_AM_OID, "hash", "i"),
        (GIST_AM_OID, "gist", "i"),
        (GIN_AM_OID, "gin", "i"),
        (SPGIST_AM_OID, "spgist", "i"),
        (3580, "brin", "i"),
        (2, "heap", "t"),
    ]
    .into_iter()
    .map(|(oid, name, kind)| vec![int(oid), text(name), text(name), text(kind)])
    .collect()
}

/// The procedural languages every PostgreSQL cluster bootstraps, with their
/// real oids. `\df+` joins `pg_proc.prolang` against this; crabka has no
/// user-defined routines, so nothing points at these rows yet.
fn pg_language_rows() -> Vec<Vec<Datum>> {
    [
        (12, "internal", false),
        (13, "c", false),
        (14, "sql", true),
        // P2: routines may be defined in PL/pgSQL even though Gres cannot run
        // them, so `pg_proc.prolang` needs a row to join against.
        (13_647, "plpgsql", true),
    ]
    .into_iter()
    .map(|(oid, name, trusted)| {
        vec![
            int(oid),
            text(name),
            int(crate::catalog_fn::BOOTSTRAP_ROLE_OID),
            Datum::Bool(false),
            Datum::Bool(trusted),
            int(0),
            int(0),
            int(0),
            Datum::Null,
        ]
    })
    .collect()
}

/// One row per column default. `adbin` holds the default's source text — crabka
/// stores defaults as text, so `pg_get_expr(adbin, adrelid)` is the identity.
fn pg_attrdef_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(kv)? {
        let relid = i32::try_from(table.id)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?;
        let keys = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.default.is_some())
            .map(|(idx, column)| format!("{}.{}.{idx}", table.name, column.name))
            .collect::<Vec<_>>();
        let oids = banded_oids(ATTRDEF_OID_BASE, &keys);
        for (idx, column) in table.columns.iter().enumerate() {
            let Some(default) = &column.default else {
                continue;
            };
            let key = format!("{}.{}.{idx}", table.name, column.name);
            let attnum = i16::try_from(idx + 1)
                .map_err(|_| ExecError::Unsupported("attnum exceeds int2 range".into()))?;
            rows.push(vec![
                int(oids.get(&key).copied().unwrap_or(0)),
                int(relid),
                small(attnum),
                text(&crate::catalog_fn::default_source_text(
                    kv, default, column.ty,
                )),
            ]);
        }
    }
    Ok(rows)
}

fn pg_authid_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let oids = role_oids(kv)?;
    Ok(crabka_pgcatalog::list_roles(kv)?
        .into_iter()
        .map(|role| (role.name, role.can_login))
        .map(|(name, can_login)| {
            vec![
                int(oids.get(&name).copied().unwrap_or(0)),
                text(&name),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(can_login),
                Datum::Bool(false),
                Datum::Bool(false),
                int(-1),
                Datum::Null,
                Datum::Null,
            ]
        })
        .collect())
}

/// The three collations every UTF-8 PostgreSQL cluster has. crabka compares
/// text with memcmp semantics, which is what `C` describes; `default` is the
/// database default and is what every column reports.
fn pg_collation_rows() -> Vec<Vec<Datum>> {
    [
        (DEFAULT_COLLATION_OID, "default", "d", 0),
        (950, "C", "c", -1),
        (951, "POSIX", "c", -1),
    ]
    .into_iter()
    .map(|(oid, name, provider, encoding)| {
        vec![
            int(oid),
            text(name),
            int(crate::exec::PG_CATALOG_NAMESPACE_OID),
            int(10),
            text(provider),
            Datum::Bool(true),
            int(encoding),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]
    })
    .collect()
}

/// The single database crabka exposes, matching what `current_database()` says.
fn pg_database_rows() -> Vec<Vec<Datum>> {
    vec![vec![
        int(DATABASE_OID),
        text(crate::exec::CURRENT_DATABASE),
        int(10),
        int(crate::catalog_fn::UTF8_ENCODING),
        text("c"),
        Datum::Bool(false),
        Datum::Bool(true),
        Datum::Bool(false),
        int(-1),
        Datum::Int8(1),
        Datum::Int8(1),
        int(DEFAULT_TABLESPACE_OID),
        text("C"),
        text("C"),
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
    ]]
}

fn pg_tablespace_rows() -> Vec<Vec<Datum>> {
    [(DEFAULT_TABLESPACE_OID, "pg_default"), (1664, "pg_global")]
        .into_iter()
        .map(|(oid, name)| vec![int(oid), text(name), int(10), Datum::Null, Datum::Null])
        .collect()
}

/// The current backend, as `pg_stat_activity` describes it. crabka has no
/// cross-session backend registry, so exactly one row is reported: the backend
/// running the query, which is what a health check or a "who am I" probe reads.
///
/// The `pid` is the querying session's backend id, so
/// `WHERE pid = pg_backend_pid()` selects the row — the pairing every
/// "am I still connected" probe rests on.
fn pg_stat_activity_rows(backend_pid: i32) -> Vec<Vec<Datum>> {
    vec![vec![
        int(DATABASE_OID),
        text(crate::exec::CURRENT_DATABASE),
        int(backend_pid),
        Datum::Null,
        int(10),
        Datum::Null,
        text("crabka"),
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        text("active"),
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        text("client backend"),
    ]]
}

/// One row per view, describing its `_RETURN` rewrite rule the way PostgreSQL
/// does. `ev_action` holds the view's stored definition text rather than a
/// serialized plan tree — nothing in crabka reads it as a node tree.
fn pg_rewrite_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let oids = view_oids(kv)?;
    Ok(crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| {
            let relid = oids.get(&view.name).copied().unwrap_or(0);
            vec![
                int(relid),
                text("_RETURN"),
                int(relid),
                text("1"),
                text("O"),
                Datum::Bool(true),
                Datum::Null,
                text(&view.definition),
            ]
        })
        .collect())
}

fn pg_sequence_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let oids = sequence_oids(kv)?;
    Ok(crabka_pgcatalog::list_sequences(kv)?
        .into_iter()
        .map(|(name, sequence)| {
            vec![
                int(oids.get(&name).copied().unwrap_or(0)),
                int(20),
                Datum::Int8(sequence.start),
                Datum::Int8(sequence.increment),
                Datum::Int8(sequence.max),
                Datum::Int8(sequence.min),
                Datum::Int8(sequence.cache),
                Datum::Bool(sequence.cycle),
            ]
        })
        .collect())
}

/// `COMMENT ON` text, as `pg_description` reports it. `classoid` names the
/// catalog the commented object lives in and `objsubid` is the column number
/// for a column comment (0 otherwise) — `obj_description`/`col_description`
/// both read exactly these two.
fn pg_description_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(kv)? {
        let relid = i32::try_from(table.id)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?;
        if let Some(comment) =
            crabka_pgcatalog::get_comment(kv, "table", CommentObject::Relation(&table.name))?
        {
            rows.push(description_row(relid, 0, &comment));
        }
        for (idx, column) in table.columns.iter().enumerate() {
            let object = CommentObject::Column(&table.name, &column.name);
            if let Some(comment) = crabka_pgcatalog::get_comment(kv, "column", object)? {
                let attnum = i32::try_from(idx + 1)
                    .map_err(|_| ExecError::Unsupported("attnum exceeds int4 range".into()))?;
                rows.push(description_row(relid, attnum, &comment));
            }
        }
    }
    let view_oids = view_oids(kv)?;
    for view in crabka_pgcatalog::list_views(kv)? {
        if let Some(comment) =
            crabka_pgcatalog::get_comment(kv, "view", CommentObject::Relation(&view.name))?
        {
            rows.push(description_row(
                view_oids.get(&view.name).copied().unwrap_or(0),
                0,
                &comment,
            ));
        }
    }
    Ok(rows)
}

fn description_row(objoid: i32, objsubid: i32, comment: &str) -> Vec<Datum> {
    vec![
        int(objoid),
        int(crate::catalog_fn::PG_CLASS_OID),
        int(objsubid),
        text(comment),
    ]
}

/// Primary-key/unique constraints (each backed by an index), `CHECK` and
/// `NOT NULL` constraints, and `FOREIGN KEY` constraints.
fn pg_constraint_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for index in crabka_pgcatalog::list_indexes(kv)? {
        let Some(kind) = index.constraint else {
            continue;
        };
        let table = crabka_pgcatalog::get_table(kv, &index.table)?;
        let conkey = index
            .columns
            .iter()
            .filter_map(|column| table.column_index(column))
            .filter_map(|idx| i16::try_from(idx + 1).ok())
            .map(Datum::Int2)
            .collect::<Vec<_>>();
        let contype = match kind {
            IndexConstraint::PrimaryKey => "p",
            IndexConstraint::Unique => "u",
            IndexConstraint::Exclusion(_) => "x",
        };
        rows.push(constraint_row(ConstraintRow {
            oid: index_constraint_oid(index.id)?,
            name: &index.name,
            schema: &index.table.schema,
            contype,
            conrelid: table_relation_oid(index.table_id)?,
            conindid: index_relation_oid(index.id)?,
            conkey: Some(conkey),
            conbin: Datum::Null,
            validated: true,
            condeferrable: false,
            condeferred: false,
            referent: Referent::default(),
        }));
    }
    rows.extend(check_constraint_rows(kv)?);
    rows.extend(foreign_key_constraint_rows(kv)?);
    Ok(rows)
}

/// `FOREIGN KEY` constraints (`contype = 'f'`).
///
/// `conindid` is the *referenced* index — the unique index that proves the
/// referenced columns are a key — and `confrelid` the referenced relation.
/// `confrelid` is load-bearing rather than cosmetic: `psql`'s `\d <parent>`
/// renders its `Referenced by:` section by filtering `pg_constraint` on it, so
/// leaving it 0 makes the parent's `\d` silently empty.
///
/// `conkey` and `confkey` are both stored in the order the FK clause wrote
/// them, paired positionally — not sorted, and not permuted into the referenced
/// index's column order.
fn foreign_key_constraint_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        let child = crabka_pgcatalog::get_table(kv, &foreign_key.table)?;
        let parent = crabka_pgcatalog::get_table(kv, &foreign_key.referenced_table)?;
        // An empty `set_columns` means the action touches every referencing
        // column, which PostgreSQL records as a NULL `confdelsetcols` rather
        // than a copy of `conkey`.
        let set_columns = if foreign_key.set_columns.is_empty() {
            None
        } else {
            Some(attnums(&child, &foreign_key.set_columns)?)
        };
        rows.push(constraint_row(ConstraintRow {
            oid: foreign_key_oid(foreign_key.id)?,
            name: &foreign_key.name,
            schema: &foreign_key.table.schema,
            contype: "f",
            conrelid: table_relation_oid(foreign_key.table_id)?,
            conindid: index_relation_oid(foreign_key.referenced_index_id)?,
            conkey: Some(attnums(&child, &foreign_key.columns)?),
            conbin: Datum::Null,
            validated: foreign_key.validated,
            condeferrable: foreign_key.deferrable,
            condeferred: foreign_key.initially_deferred,
            referent: Referent {
                confrelid: table_relation_oid(foreign_key.referenced_table_id)?,
                confupdtype: referential_action_code(foreign_key.on_update),
                confdeltype: referential_action_code(foreign_key.on_delete),
                confmatchtype: match_type_code(foreign_key.match_type),
                confkey: Some(attnums(&parent, &foreign_key.referenced_columns)?),
                confdelsetcols: set_columns,
            },
        }));
    }
    Ok(rows)
}

/// The 1-based attnums of `columns` in `table`, in the order written — the
/// contents of a `pg_constraint` attnum array.
fn attnums(table: &Table, columns: &[String]) -> Result<Vec<Datum>, ExecError> {
    columns
        .iter()
        .map(|column| {
            let position =
                table
                    .column_index(column)
                    .ok_or_else(|| ExecError::UndefinedTableColumn {
                        column: column.clone(),
                        table: table.name.to_string(),
                    })?;
            i16::try_from(position + 1)
                .map(Datum::Int2)
                .map_err(|_| ExecError::Unsupported("attnum exceeds int2 range".into()))
        })
        .collect()
}

/// The `"char"` PostgreSQL stores in `confupdtype`/`confdeltype` for a
/// referential action.
fn referential_action_code(action: ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "a",
        ReferentialAction::Restrict => "r",
        ReferentialAction::Cascade => "c",
        ReferentialAction::SetNull => "n",
        ReferentialAction::SetDefault => "d",
    }
}

/// The `"char"` PostgreSQL stores in `confmatchtype`. `MATCH PARTIAL`'s `p` has
/// no crabka spelling because the parser refuses it.
fn match_type_code(match_type: MatchType) -> &'static str {
    match match_type {
        MatchType::Simple => "s",
        MatchType::Full => "f",
    }
}

/// `CHECK` constraints (`contype = 'c'`) and the `NOT NULL` constraints
/// PostgreSQL 18 records alongside them (`contype = 'n'`, named
/// `<table>_<column>_not_null`).
fn check_constraint_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let check_oids = check_constraint_oids(kv)?;
    let not_null_oids = not_null_constraint_oids(kv)?;
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(kv)? {
        let relid = table_relation_oid(table.id)?;
        for check in &table.checks {
            let key = ConstraintKey::new(&table.name, &check.name);
            rows.push(constraint_row(ConstraintRow {
                oid: check_oids.get(&key).copied().unwrap_or(0),
                name: &check.name,
                schema: &table.name.schema,
                contype: "c",
                conrelid: relid,
                conindid: 0,
                conkey: None,
                conbin: text(&check.expr),
                validated: check.validated,
                condeferrable: false,
                condeferred: false,
                referent: Referent::default(),
            }));
        }
        for (idx, column) in table.columns.iter().enumerate() {
            if !column.not_null {
                continue;
            }
            let key = ConstraintKey::new(&table.name, &column.name);
            let attnum = i16::try_from(idx + 1)
                .map_err(|_| ExecError::Unsupported("attnum exceeds int2 range".into()))?;
            rows.push(constraint_row(ConstraintRow {
                oid: not_null_oids.get(&key).copied().unwrap_or(0),
                name: &not_null_constraint_name(&table.name, &column.name),
                schema: &table.name.schema,
                contype: "n",
                conrelid: relid,
                conindid: 0,
                conkey: Some(vec![Datum::Int2(attnum)]),
                conbin: Datum::Null,
                validated: true,
                condeferrable: false,
                condeferred: false,
                referent: Referent::default(),
            }));
        }
    }
    Ok(rows)
}

/// The name PostgreSQL 18 gives the `pg_constraint` row it records for a
/// `NOT NULL` column: the *unqualified* table name, an underscore, the column
/// and `_not_null`. A constraint name is never schema-qualified — the schema
/// lives in `connamespace` instead.
fn not_null_constraint_name(table: &RelationName, column: &str) -> String {
    format!("{}_{column}_not_null", table.name)
}

/// The `pg_constraint` fields that vary by constraint kind; the rest of the
/// wide tuple is the same for every row crabka produces.
struct ConstraintRow<'a> {
    oid: i32,
    name: &'a str,
    /// The schema of the relation the constraint is on, which is the one
    /// `connamespace` names — a constraint has no schema of its own.
    schema: &'a str,
    contype: &'a str,
    conrelid: i32,
    conindid: i32,
    conkey: Option<Vec<Datum>>,
    conbin: Datum,
    validated: bool,
    condeferrable: bool,
    condeferred: bool,
    referent: Referent,
}

/// The `conf*` columns, which only a `FOREIGN KEY` fills in. [`Default`] is
/// PostgreSQL's "references nothing" spelling: `confrelid` 0, a single space in
/// each of the three `"char"` codes, and NULL attnum arrays.
struct Referent {
    confrelid: i32,
    confupdtype: &'static str,
    confdeltype: &'static str,
    confmatchtype: &'static str,
    confkey: Option<Vec<Datum>>,
    confdelsetcols: Option<Vec<Datum>>,
}

impl Default for Referent {
    fn default() -> Self {
        Self {
            confrelid: 0,
            confupdtype: NO_REFERENT_CODE,
            confdeltype: NO_REFERENT_CODE,
            confmatchtype: NO_REFERENT_CODE,
            confkey: None,
            confdelsetcols: None,
        }
    }
}

/// The `"char"` PostgreSQL leaves in the referential code columns of a
/// constraint that references nothing.
const NO_REFERENT_CODE: &str = " ";

/// An `int2` attnum array column, or NULL where the constraint has no such list.
fn attnum_array(attnums: Option<Vec<Datum>>) -> Datum {
    attnums.map_or(Datum::Null, |elems| {
        Datum::Array(crabka_pgtypes::ArrayValue::new(ElemType::Int2, elems))
    })
}

/// One `pg_constraint` tuple, in PostgreSQL 18.4's 28-column order.
fn constraint_row(row: ConstraintRow<'_>) -> Vec<Datum> {
    let referent = row.referent;
    vec![
        int(row.oid),
        text(row.name),
        int(namespace_oid(row.schema)),
        text(row.contype),
        Datum::Bool(row.condeferrable),
        Datum::Bool(row.condeferred),
        Datum::Bool(true),
        Datum::Bool(row.validated),
        int(row.conrelid),
        int(0),
        int(row.conindid),
        int(0),
        int(referent.confrelid),
        text(referent.confupdtype),
        text(referent.confdeltype),
        text(referent.confmatchtype),
        Datum::Bool(true),
        small(0),
        Datum::Bool(false),
        Datum::Bool(false),
        attnum_array(row.conkey),
        attnum_array(referent.confkey),
        Datum::Null,
        Datum::Null,
        Datum::Null,
        attnum_array(referent.confdelsetcols),
        Datum::Null,
        row.conbin,
    ]
}

fn pg_indexes_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    crabka_pgcatalog::list_indexes(kv)?
        .into_iter()
        .map(|index| {
            let table = crabka_pgcatalog::get_table(kv, &index.table)?;
            Ok(vec![
                // An index lives in the schema of the table it indexes.
                text(&index.table.schema),
                text(&index.table.name),
                text(&index.name),
                Datum::Null,
                text(&crate::catalog_fn::index_definition(&index, &table)),
            ])
        })
        .collect()
}

fn pg_tables_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let indexed = crabka_pgcatalog::list_indexes(kv)?
        .into_iter()
        .map(|index| index.table)
        .collect::<std::collections::BTreeSet<_>>();
    Ok(crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .filter(|table| table.foreign.is_none())
        .map(|table| {
            vec![
                text(&table.name.schema),
                text(&table.name.name),
                text(crate::catalog_fn::OBJECT_OWNER),
                Datum::Null,
                Datum::Bool(indexed.contains(&table.name)),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(false),
            ]
        })
        .collect())
}

fn pg_views_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| {
            vec![
                text(&view.name.schema),
                text(&view.name.name),
                text(crate::catalog_fn::OBJECT_OWNER),
                text(&crate::catalog_fn::view_definition_text(&view, false)),
            ]
        })
        .collect())
}

// -------------------------------------------------------- information_schema

/// Column lists for the SQL-standard views, in PostgreSQL 18.4 order. Every
/// column is `text` in the standard's `sql_identifier`/`character_data`
/// domains except the explicitly numeric ones.
fn information_schema_columns(name: &str) -> Vec<Column> {
    use ColumnType::{Int4, Text};
    match name {
        "information_schema.table_constraints" => cols(&[
            ("constraint_catalog", Text),
            ("constraint_schema", Text),
            ("constraint_name", Text),
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("constraint_type", Text),
            ("is_deferrable", Text),
            ("initially_deferred", Text),
            ("enforced", Text),
            ("nulls_distinct", Text),
        ]),
        "information_schema.key_column_usage" => cols(&[
            ("constraint_catalog", Text),
            ("constraint_schema", Text),
            ("constraint_name", Text),
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("column_name", Text),
            ("ordinal_position", Int4),
            ("position_in_unique_constraint", Int4),
        ]),
        "information_schema.constraint_column_usage" => cols(&[
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("column_name", Text),
            ("constraint_catalog", Text),
            ("constraint_schema", Text),
            ("constraint_name", Text),
        ]),
        "information_schema.referential_constraints" => cols(&[
            ("constraint_catalog", Text),
            ("constraint_schema", Text),
            ("constraint_name", Text),
            ("unique_constraint_catalog", Text),
            ("unique_constraint_schema", Text),
            ("unique_constraint_name", Text),
            ("match_option", Text),
            ("update_rule", Text),
            ("delete_rule", Text),
        ]),
        "information_schema.views" => cols(&[
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("view_definition", Text),
            ("check_option", Text),
            ("is_updatable", Text),
            ("is_insertable_into", Text),
            ("is_trigger_updatable", Text),
            ("is_trigger_deletable", Text),
            ("is_trigger_insertable_into", Text),
        ]),
        "information_schema.enabled_roles" => cols(&[("role_name", Text)]),
        "information_schema.applicable_roles" => cols(&[
            ("grantee", Text),
            ("role_name", Text),
            ("is_grantable", Text),
        ]),
        _ => information_schema_columns_rest(name),
    }
}

/// The remaining SQL-standard views' column lists.
fn information_schema_columns_rest(name: &str) -> Vec<Column> {
    use ColumnType::{Int4, Text};
    match name {
        "information_schema.routines" => cols(&[
            ("specific_catalog", Text),
            ("specific_schema", Text),
            ("specific_name", Text),
            ("routine_catalog", Text),
            ("routine_schema", Text),
            ("routine_name", Text),
            ("routine_type", Text),
            ("data_type", Text),
            ("type_udt_catalog", Text),
            ("type_udt_schema", Text),
            ("type_udt_name", Text),
            ("routine_body", Text),
            ("routine_definition", Text),
            ("external_language", Text),
            ("is_deterministic", Text),
            ("security_type", Text),
        ]),
        "information_schema.parameters" => cols(&[
            ("specific_catalog", Text),
            ("specific_schema", Text),
            ("specific_name", Text),
            ("ordinal_position", Int4),
            ("parameter_mode", Text),
            ("is_result", Text),
            ("as_locator", Text),
            ("parameter_name", Text),
            ("data_type", Text),
            ("udt_catalog", Text),
            ("udt_schema", Text),
            ("udt_name", Text),
            ("parameter_default", Text),
        ]),
        "information_schema.sequences" => cols(&[
            ("sequence_catalog", Text),
            ("sequence_schema", Text),
            ("sequence_name", Text),
            ("data_type", Text),
            ("numeric_precision", Int4),
            ("numeric_precision_radix", Int4),
            ("numeric_scale", Int4),
            ("start_value", Text),
            ("minimum_value", Text),
            ("maximum_value", Text),
            ("increment", Text),
            ("cycle_option", Text),
        ]),
        "information_schema.table_privileges" => cols(&[
            ("grantor", Text),
            ("grantee", Text),
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("privilege_type", Text),
            ("is_grantable", Text),
            ("with_hierarchy", Text),
        ]),
        "information_schema.column_privileges" => cols(&[
            ("grantor", Text),
            ("grantee", Text),
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("column_name", Text),
            ("privilege_type", Text),
            ("is_grantable", Text),
        ]),
        _ => Vec::new(),
    }
}

/// Rows for the SQL-standard views.
fn information_schema_rows(kv: &dyn Kv, name: &str) -> Result<Vec<Vec<Datum>>, ExecError> {
    match name {
        "information_schema.table_constraints" => table_constraint_rows(kv),
        "information_schema.key_column_usage" => key_column_usage_rows(kv),
        "information_schema.constraint_column_usage" => constraint_column_usage_rows(kv),
        "information_schema.referential_constraints" => referential_constraint_rows(kv),
        "information_schema.views" => information_schema_view_rows(kv),
        "information_schema.enabled_roles" => enabled_role_rows(kv),
        "information_schema.applicable_roles" => Ok(Vec::new()),
        "information_schema.sequences" => sequence_view_rows(kv),
        "information_schema.table_privileges" => table_privilege_rows(kv),
        "information_schema.column_privileges" => Ok(Vec::new()),
        // `routines`/`parameters` need user-defined routines, which crabka has
        // no object kind for yet, so both are correctly empty rather than
        // absent.
        _ => Ok(Vec::new()),
    }
}

fn catalog_name() -> Datum {
    text(crate::exec::CURRENT_DATABASE)
}

/// A constraint's `information_schema` identity: catalog, schema, name.
///
/// A constraint has no schema of its own — it belongs to the schema of the
/// relation it constrains, which is what `pg_constraint.connamespace` records
/// and what the standard views report as `constraint_schema`.
fn constraint_identity(schema: &str, name: &str) -> [Datum; 3] {
    [catalog_name(), text(schema), text(name)]
}

/// A relation's `information_schema` identity: catalog, schema, name — the
/// `table_catalog`/`table_schema`/`table_name` triple every standard view
/// carries, reporting the relation's real schema.
fn relation_identity(relation: &RelationName) -> [Datum; 3] {
    [catalog_name(), text(&relation.schema), text(&relation.name)]
}

/// The `information_schema` spelling of a boolean-valued `character_data` column.
fn yes_no(flag: bool) -> &'static str {
    if flag { "YES" } else { "NO" }
}

/// The 1-based `information_schema` ordinal of a 0-based position.
fn ordinal(position: usize) -> Result<i32, ExecError> {
    i32::try_from(position + 1)
        .map_err(|_| ExecError::Unsupported("ordinal exceeds int4 range".into()))
}

fn table_constraint_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for index in crabka_pgcatalog::list_indexes(kv)? {
        let Some(kind) = index.constraint else {
            continue;
        };
        let constraint_type = match kind {
            IndexConstraint::PrimaryKey => "PRIMARY KEY",
            IndexConstraint::Unique => "UNIQUE",
            IndexConstraint::Exclusion(_) => "EXCLUDE",
        };
        rows.push(table_constraint_row(
            &index.name,
            &index.table,
            constraint_type,
            false,
            false,
        ));
    }
    for table in crabka_pgcatalog::list_tables(kv)? {
        for check in &table.checks {
            rows.push(table_constraint_row(
                &check.name,
                &table.name,
                "CHECK",
                false,
                false,
            ));
        }
    }
    // A foreign key is the one constraint kind crabka can defer, so it is the
    // one that reports anything but NO/NO.
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        rows.push(table_constraint_row(
            &foreign_key.name,
            &foreign_key.table,
            "FOREIGN KEY",
            foreign_key.deferrable,
            foreign_key.initially_deferred,
        ));
    }
    Ok(rows)
}

fn table_constraint_row(
    name: &str,
    table: &RelationName,
    constraint_type: &str,
    deferrable: bool,
    initially_deferred: bool,
) -> Vec<Datum> {
    let mut row = constraint_identity(&table.schema, name).to_vec();
    row.extend(relation_identity(table));
    row.extend([
        text(constraint_type),
        text(yes_no(deferrable)),
        text(yes_no(initially_deferred)),
        text("YES"),
        if constraint_type == "UNIQUE" {
            text("YES")
        } else {
            Datum::Null
        },
    ]);
    row
}

/// `information_schema.referential_constraints`: one row per foreign key.
///
/// `unique_constraint_catalog`/`_schema`/`_name` are NULL as a triple when the
/// referent is a bare `CREATE UNIQUE INDEX` rather than a `PRIMARY KEY` or
/// `UNIQUE` constraint — PostgreSQL's view LEFT JOINs `pg_constraint` to name
/// it, so an index with no constraint marker yields no name.
fn referential_constraint_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        let referent = crabka_pgcatalog::get_index(kv, &referenced_index_name(&foreign_key))?;
        let mut row = constraint_identity(&foreign_key.table.schema, &foreign_key.name).to_vec();
        if referent.constraint.is_some() {
            row.extend(constraint_identity(&referent.table.schema, &referent.name));
        } else {
            row.extend([Datum::Null, Datum::Null, Datum::Null]);
        }
        row.extend([
            text(match_option(foreign_key.match_type)),
            text(referential_action_rule(foreign_key.on_update)),
            text(referential_action_rule(foreign_key.on_delete)),
        ]);
        rows.push(row);
    }
    Ok(rows)
}

/// The catalog name of the unique index a foreign key references. A foreign key
/// stores that index's bare name, and an index lives in the schema of the table
/// it indexes — here, the referenced table's.
fn referenced_index_name(foreign_key: &crabka_pgcatalog::ForeignKey) -> RelationName {
    foreign_key
        .referenced_table
        .sibling(&foreign_key.referenced_index)
}

/// The SQL standard's `match_option`, whose spelling for `MATCH SIMPLE` is
/// `NONE` rather than `SIMPLE`.
fn match_option(match_type: MatchType) -> &'static str {
    match match_type {
        MatchType::Simple => "NONE",
        MatchType::Full => "FULL",
    }
}

/// The SQL standard's `update_rule`/`delete_rule` — the referential action
/// spelled out rather than the `pg_constraint` `"char"`.
fn referential_action_rule(action: ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "NO ACTION",
        ReferentialAction::Restrict => "RESTRICT",
        ReferentialAction::Cascade => "CASCADE",
        ReferentialAction::SetNull => "SET NULL",
        ReferentialAction::SetDefault => "SET DEFAULT",
    }
}

fn key_column_usage_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if index.constraint.is_none() {
            continue;
        }
        for (position, column) in index.columns.iter().enumerate() {
            let mut row = constraint_identity(&index.table.schema, &index.name).to_vec();
            row.extend(relation_identity(&index.table));
            row.extend([text(column), int(ordinal(position)?), Datum::Null]);
            rows.push(row);
        }
    }
    rows.extend(foreign_key_column_usage_rows(kv)?);
    Ok(rows)
}

/// A foreign key's *referencing* columns, which PostgreSQL includes here
/// because its `key_column_usage` covers `contype IN ('p', 'u', 'f')`.
///
/// `position_in_unique_constraint` is the paired referenced column's position
/// within the referenced *index*, which is why it can disagree with
/// `ordinal_position`: a permuted composite key pairs by written order while
/// the index keeps its own.
fn foreign_key_column_usage_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        let referent = crabka_pgcatalog::get_index(kv, &referenced_index_name(&foreign_key))?;
        for (position, column) in foreign_key.columns.iter().enumerate() {
            let paired = foreign_key
                .referenced_columns
                .get(position)
                .and_then(|name| referent.columns.iter().position(|keyed| keyed == name));
            let in_unique = match paired {
                Some(keyed) => int(ordinal(keyed)?),
                None => Datum::Null,
            };
            let mut row =
                constraint_identity(&foreign_key.table.schema, &foreign_key.name).to_vec();
            row.extend(relation_identity(&foreign_key.table));
            row.extend([text(column), int(ordinal(position)?), in_unique]);
            rows.push(row);
        }
    }
    Ok(rows)
}

fn constraint_column_usage_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if index.constraint.is_none() {
            continue;
        }
        for column in &index.columns {
            rows.push(column_usage_row(
                &index.table,
                column,
                &index.table.schema,
                &index.name,
            ));
        }
    }
    // PostgreSQL 18 records `NOT NULL` in `pg_constraint`, so it shows up here
    // too, one row per constrained column.
    for table in crabka_pgcatalog::list_tables(kv)? {
        for column in table.columns.iter().filter(|column| column.not_null) {
            let name = not_null_constraint_name(&table.name, &column.name);
            rows.push(column_usage_row(
                &table.name,
                &column.name,
                &table.name.schema,
                &name,
            ));
        }
    }
    // A foreign key is attributed to the columns it *references*, on the parent
    // relation — the mirror of `key_column_usage`, which lists the child's.
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        for column in &foreign_key.referenced_columns {
            rows.push(column_usage_row(
                &foreign_key.referenced_table,
                column,
                &foreign_key.table.schema,
                &foreign_key.name,
            ));
        }
    }
    Ok(rows)
}

/// One `constraint_column_usage` row. The relation and the constraint carry
/// their own schemas because a foreign key separates them: the columns are the
/// *parent's*, while the constraint belongs to the child's schema.
fn column_usage_row(
    table: &RelationName,
    column: &str,
    constraint_schema: &str,
    constraint: &str,
) -> Vec<Datum> {
    let mut row = relation_identity(table).to_vec();
    row.push(text(column));
    row.extend(constraint_identity(constraint_schema, constraint));
    row
}

fn information_schema_view_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| {
            let updatable = crate::catalog_fn::view_is_auto_updatable(&view);
            let flag = if updatable { "YES" } else { "NO" };
            let view_id = crate::catalog_rel::view_oids(kv)?
                .get(&view.name)
                .copied()
                .and_then(|oid| u32::try_from(oid).ok())
                .unwrap_or(0);
            let triggers = crabka_pgcatalog::trigger::triggers_for_table(kv, view_id)?;
            let instead = |matches: fn(&crabka_pgcatalog::trigger::TriggerEvents) -> bool| {
                if triggers.iter().any(|trigger| {
                    trigger.timing == crabka_pgcatalog::trigger::TriggerTiming::InsteadOf
                        && trigger.level == crabka_pgcatalog::trigger::TriggerLevel::Row
                        && matches(&trigger.events)
                }) {
                    "YES"
                } else {
                    "NO"
                }
            };
            let mut row = relation_identity(&view.name).to_vec();
            row.extend([
                text(&crate::catalog_fn::view_definition_text(&view, false)),
                text("NONE"),
                text(flag),
                text(flag),
                text(instead(|events| events.update)),
                text(instead(|events| events.delete)),
                text(instead(|events| events.insert)),
            ]);
            Ok(row)
        })
        .collect()
}

fn enabled_role_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_roles(kv)?
        .into_iter()
        .map(|role| vec![text(&role.name)])
        .collect())
}

fn sequence_view_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_sequences(kv)?
        .into_iter()
        .map(|(name, sequence)| {
            let mut row = relation_identity(&name).to_vec();
            row.extend([
                text("bigint"),
                int(64),
                int(2),
                int(0),
                text(&sequence.start.to_string()),
                text(&sequence.min.to_string()),
                text(&sequence.max.to_string()),
                text(&sequence.increment.to_string()),
                text(if sequence.cycle { "YES" } else { "NO" }),
            ]);
            row
        })
        .collect())
}

/// The owner's implicit grants on every table it owns, then any explicit
/// `GRANT`. PostgreSQL lists the owner's seven table privileges in ACL bit
/// order, grantable, and `with_hierarchy` only for `SELECT`.
fn table_privilege_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    const OWNER_PRIVILEGES: [&str; 7] = [
        "INSERT",
        "SELECT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
    ];
    let owner = crate::catalog_fn::OBJECT_OWNER;
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(kv)? {
        for privilege in OWNER_PRIVILEGES {
            rows.push(privilege_row(owner, &table.name, privilege, true));
        }
    }
    for privilege in crabka_pgcatalog::list_table_privileges(kv)? {
        rows.push(privilege_row(
            &privilege.grantee,
            &privilege.table,
            &privilege.privilege.to_ascii_uppercase(),
            false,
        ));
    }
    Ok(rows)
}

fn privilege_row(grantee: &str, table: &RelationName, privilege: &str, owned: bool) -> Vec<Datum> {
    let mut row = vec![text(crate::catalog_fn::OBJECT_OWNER), text(grantee)];
    row.extend(relation_identity(table));
    row.extend([
        text(privilege),
        text(if owned { "YES" } else { "NO" }),
        text(if owned && privilege == "SELECT" {
            "YES"
        } else {
            "NO"
        }),
    ]);
    row
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{ForeignKey, IndexId, IndexMethod, IndexPlacement, NewIndex, TableId};
    use crabka_pgkv::MemKv;
    use crabka_pgtypes::ArrayValue;

    use super::*;

    #[test]
    fn every_named_relation_resolves_qualified_and_bare() {
        for name in relation_names() {
            let bare = name
                .strip_prefix("information_schema.")
                .map_or(*name, |_| *name);
            assert!(catalog_relation(bare) == Some(*name));
            let qualified = if name.starts_with("information_schema.") {
                (*name).to_string()
            } else {
                format!("pg_catalog.{name}")
            };
            assert!(catalog_relation(&qualified) == Some(*name));
        }
    }

    #[test]
    fn every_named_relation_has_columns_and_a_distinct_oid() {
        let mut oids = std::collections::BTreeSet::new();
        for name in relation_names() {
            assert!(!columns(name).is_empty(), "{name} has no columns");
            let oid = relation_oid(name);
            assert!(oid != 0, "{name} has no oid");
            assert!(oids.insert(oid), "{name} reuses oid {oid}");
        }
    }

    #[test]
    fn banded_oids_are_stable_under_unrelated_additions() {
        let first = banded_oids(1_000, &["a".to_string(), "b".to_string()]);
        let second = banded_oids(1_000, &["a".to_string(), "b".to_string(), "c".to_string()]);
        assert!(first["a"] == second["a"]);
        assert!(first["b"] == second["b"]);
    }

    /// Two relations of the same name in different schemas are two objects, so
    /// the band has to see the schema — the `public`-bare display spelling
    /// would give both the same slot.
    #[test]
    fn banded_oids_separate_same_named_relations_in_different_schemas() {
        let names = [
            RelationName::public("t"),
            RelationName::new("app", "t"),
            RelationName::new("other", "t"),
        ];

        let assigned = banded_oids(1_000, &names);

        let distinct = assigned.values().collect::<std::collections::BTreeSet<_>>();
        assert!(distinct.len() == names.len());
    }

    #[test]
    fn banded_oids_are_distinct_for_distinct_names() {
        let names = (0..200).map(|n| format!("t{n}")).collect::<Vec<_>>();
        let assigned = banded_oids(1_000, &names);
        let distinct = assigned.values().collect::<std::collections::BTreeSet<_>>();
        assert!(distinct.len() == names.len());
    }

    /// Every projection that carries a schema reports the relation's own,
    /// rather than the `public` the catalog assumed before a relation had one.
    /// An index is reported in its table's schema, which is where PostgreSQL
    /// puts it.
    #[test]
    fn projections_report_a_relations_real_schema() {
        const VIEW: &str = "information_schema.table_constraints";
        let kv = MemKv::default();
        let ops = crabka_pgcatalog::create_schema_ops(&kv, "app", "postgres").expect("schema ops");
        kv.write_batch(&ops).expect("write schema");
        let table = RelationName::new("app", "t");
        crabka_pgcatalog::create_table(&kv, &table, vec![int4("id")]).expect("t");
        add_index(
            &kv,
            &table,
            "t_pkey",
            &["id"],
            Some(IndexConstraint::PrimaryKey),
        );

        let tables = rows(&kv, "pg_tables", 0).expect("pg_tables");
        let indexes = rows(&kv, "pg_indexes", 0).expect("pg_indexes");
        let constraints = rows(&kv, "pg_constraint", 0).expect("pg_constraint");
        let standard = rows(&kv, VIEW, 0).expect("table_constraints");

        assert!(field("pg_tables", &tables[0], "schemaname") == text("app"));
        assert!(field("pg_indexes", &indexes[0], "schemaname") == text("app"));
        assert!(
            field("pg_indexes", &indexes[0], "indexdef")
                == text("CREATE UNIQUE INDEX t_pkey ON app.t USING btree (id)")
        );
        let pkey = row_named("pg_constraint", &constraints, "conname", "t_pkey");
        assert!(field("pg_constraint", &pkey, "connamespace") == int(namespace_oid("app")));
        let reported = row_named(VIEW, &standard, "constraint_name", "t_pkey");
        assert!(field(VIEW, &reported, "constraint_schema") == text("app"));
        assert!(field(VIEW, &reported, "table_schema") == text("app"));
    }

    // ------------------------------------------------------------ foreign keys

    /// The oracle's shapes: a `pp(id, k, m)` parent carrying a primary key on
    /// `id`, a `UNIQUE` constraint on `k`, a composite `UNIQUE` constraint on
    /// `(id, k)` and a bare `CREATE UNIQUE INDEX` on `m`; plus a `cc(a, b, c)`
    /// child to hang foreign keys off.
    struct Schema {
        parent: TableId,
        child: TableId,
        pkey: IndexId,
        unique: IndexId,
        composite: IndexId,
        bare: IndexId,
    }

    fn int4(name: &str) -> Column {
        Column::new(name, ColumnType::Int4)
    }

    fn add_index(
        kv: &MemKv,
        table: &RelationName,
        name: &str,
        index_columns: &[&str],
        constraint: Option<IndexConstraint>,
    ) -> IndexId {
        let table = crabka_pgcatalog::get_table(kv, table).expect("table");
        let (index, ops) = crabka_pgcatalog::create_constraint_index_ops(
            kv,
            &table,
            &NewIndex {
                name: name.to_string(),
                columns: index_columns.iter().map(|c| (*c).to_string()).collect(),
                unique: true,
                method: IndexMethod::Btree,
                placement: IndexPlacement::Local,
                constraint,
            },
        )
        .expect("index ops");
        kv.write_batch(&ops).expect("write index");
        index.id
    }

    fn add_foreign_key(kv: &MemKv, foreign_key: &ForeignKey) {
        let ops = crabka_pgcatalog::create_foreign_key_ops(kv, foreign_key).expect("fk ops");
        kv.write_batch(&ops).expect("write fk");
    }

    fn oracle_schema(kv: &MemKv) -> Schema {
        let pp = RelationName::public("pp");
        let parent =
            crabka_pgcatalog::create_table(kv, &pp, vec![int4("id"), int4("k"), int4("m")])
                .expect("pp");
        let child = crabka_pgcatalog::create_table(
            kv,
            &RelationName::public("cc"),
            vec![int4("a"), int4("b"), int4("c")],
        )
        .expect("cc");
        Schema {
            parent,
            child,
            pkey: add_index(
                kv,
                &pp,
                "pp_pkey",
                &["id"],
                Some(IndexConstraint::PrimaryKey),
            ),
            unique: add_index(kv, &pp, "pp_k_key", &["k"], Some(IndexConstraint::Unique)),
            composite: add_index(
                kv,
                &pp,
                "pp_id_k_key",
                &["id", "k"],
                Some(IndexConstraint::Unique),
            ),
            bare: add_index(kv, &pp, "pp_m_idx", &["m"], None),
        }
    }

    /// `cc(a) REFERENCES pp(id)` with every option at its default — the shape
    /// each case below varies one or two fields of. `id` is the creation-order
    /// id DDL would have allocated, and is what the constraint's oid derives
    /// from, so every constraint in one fixture needs its own.
    fn base_foreign_key(schema: &Schema, id: ForeignKeyId, name: &str) -> ForeignKey {
        ForeignKey {
            id,
            name: name.to_string(),
            table: RelationName::public("cc"),
            table_id: schema.child,
            columns: vec!["a".to_string()],
            referenced_table: RelationName::public("pp"),
            referenced_table_id: schema.parent,
            referenced_columns: vec!["id".to_string()],
            referenced_index: "pp_pkey".to_string(),
            referenced_index_id: schema.pkey,
            match_type: MatchType::Simple,
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            set_columns: Vec::new(),
            deferrable: false,
            initially_deferred: false,
            validated: true,
        }
    }

    /// The four foreign keys the PostgreSQL 18.4 oracle captured `pg_constraint`
    /// values for.
    fn oracle_foreign_keys(schema: &Schema) -> Vec<ForeignKey> {
        vec![
            base_foreign_key(schema, 1, "cc_a_fkey"),
            ForeignKey {
                columns: vec!["c".to_string()],
                referenced_columns: vec!["k".to_string()],
                referenced_index: "pp_k_key".to_string(),
                referenced_index_id: schema.unique,
                on_delete: ReferentialAction::SetDefault,
                deferrable: true,
                initially_deferred: true,
                ..base_foreign_key(schema, 2, "cc_def")
            },
            ForeignKey {
                columns: vec!["b".to_string()],
                match_type: MatchType::Full,
                on_update: ReferentialAction::Cascade,
                on_delete: ReferentialAction::SetNull,
                ..base_foreign_key(schema, 3, "cc_full")
            },
            ForeignKey {
                on_delete: ReferentialAction::Restrict,
                validated: false,
                ..base_foreign_key(schema, 4, "cc_nv")
            },
        ]
    }

    fn oracle_catalog() -> (MemKv, Schema) {
        let kv = MemKv::default();
        let schema = oracle_schema(&kv);
        for foreign_key in oracle_foreign_keys(&schema) {
            add_foreign_key(&kv, &foreign_key);
        }
        (kv, schema)
    }

    /// `cperm(a, b)` with `FOREIGN KEY (b, a) REFERENCES pperm(y, x)`, where
    /// `pperm`'s primary key is `(x, y)` — the oracle's permuted composite.
    fn permuted_catalog() -> MemKv {
        let kv = MemKv::default();
        let pperm = RelationName::public("pperm");
        let parent =
            crabka_pgcatalog::create_table(&kv, &pperm, vec![int4("x"), int4("y")]).expect("pperm");
        let child = crabka_pgcatalog::create_table(
            &kv,
            &RelationName::public("cperm"),
            vec![int4("a"), int4("b")],
        )
        .expect("cperm");
        let pkey = add_index(
            &kv,
            &pperm,
            "pperm_pkey",
            &["x", "y"],
            Some(IndexConstraint::PrimaryKey),
        );
        add_foreign_key(
            &kv,
            &ForeignKey {
                id: 1,
                name: "cperm_b_a_fkey".to_string(),
                table: RelationName::public("cperm"),
                table_id: child,
                columns: vec!["b".to_string(), "a".to_string()],
                referenced_table: RelationName::public("pperm"),
                referenced_table_id: parent,
                referenced_columns: vec!["y".to_string(), "x".to_string()],
                referenced_index: "pperm_pkey".to_string(),
                referenced_index_id: pkey,
                match_type: MatchType::Simple,
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
                set_columns: Vec::new(),
                deferrable: false,
                initially_deferred: false,
                validated: true,
            },
        );
        kv
    }

    /// One column of a row, located by *name* in the relation's declared column
    /// list, so a positional mistake surfaces as a value mismatch.
    fn field(relation: &str, row: &[Datum], column: &str) -> Datum {
        let position = columns(relation)
            .iter()
            .position(|declared| declared.name == column)
            .unwrap_or_else(|| panic!("{relation} has no column {column}"));
        row[position].clone()
    }

    fn rows_named(relation: &str, all: &[Vec<Datum>], key: &str, name: &str) -> Vec<Vec<Datum>> {
        all.iter()
            .filter(|row| field(relation, row, key) == text(name))
            .cloned()
            .collect()
    }

    fn row_named(relation: &str, all: &[Vec<Datum>], key: &str, name: &str) -> Vec<Datum> {
        let mut found = rows_named(relation, all, key, name);
        assert!(found.len() == 1, "{relation} has no single row for {name}");
        found.remove(0)
    }

    fn int2s(attnums: &[i16]) -> Datum {
        Datum::Array(ArrayValue::new(
            ElemType::Int2,
            attnums.iter().copied().map(Datum::Int2).collect(),
        ))
    }

    fn oid_of(table_id: TableId) -> Datum {
        int(i32::try_from(table_id).expect("relation oid"))
    }

    /// Every `pg_constraint` column of a foreign-key row, in PostgreSQL 18.4's
    /// 28-column order — the case that pins each position rather than trusting
    /// a remembered index.
    #[test]
    fn pg_constraint_foreign_key_row_matches_postgresql_column_for_column() {
        let (kv, schema) = oracle_catalog();
        let oids = foreign_key_constraint_oids(&kv).expect("oids");
        let all = rows(&kv, "pg_constraint", 0).expect("rows");

        let row = row_named("pg_constraint", &all, "conname", "cc_a_fkey");

        assert!(row.len() == columns("pg_constraint").len());
        assert!(
            row == vec![
                int(oids["cc.cc_a_fkey"]),
                text("cc_a_fkey"),
                int(crate::exec::PUBLIC_NAMESPACE_OID),
                text("f"),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Bool(true),
                oid_of(schema.child),
                int(0),
                int(index_relation_oid(schema.pkey).expect("conindid")),
                int(0),
                oid_of(schema.parent),
                text("a"),
                text("a"),
                text("s"),
                Datum::Bool(true),
                small(0),
                Datum::Bool(false),
                Datum::Bool(false),
                int2s(&[1]),
                int2s(&[1]),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]
        );
    }

    /// The `pg_constraint` columns the oracle tabulated for a foreign key.
    #[derive(Debug, PartialEq, Eq)]
    struct OracleFacts {
        condeferrable: Datum,
        condeferred: Datum,
        convalidated: Datum,
        confrelid: Datum,
        confupdtype: Datum,
        confdeltype: Datum,
        confmatchtype: Datum,
        conkey: Datum,
        confkey: Datum,
        confdelsetcols: Datum,
    }

    fn oracle_facts(row: &[Datum]) -> OracleFacts {
        let at = |column: &str| field("pg_constraint", row, column);
        OracleFacts {
            condeferrable: at("condeferrable"),
            condeferred: at("condeferred"),
            convalidated: at("convalidated"),
            confrelid: at("confrelid"),
            confupdtype: at("confupdtype"),
            confdeltype: at("confdeltype"),
            confmatchtype: at("confmatchtype"),
            conkey: at("conkey"),
            confkey: at("confkey"),
            confdelsetcols: at("confdelsetcols"),
        }
    }

    /// The oracle's four rows verbatim: `NOT VALID` clears `convalidated`,
    /// `DEFERRABLE INITIALLY DEFERRED` sets both deferral flags, and every row
    /// points `confrelid` at the parent so `\d pp`'s `Referenced by:` finds it.
    #[test]
    fn pg_constraint_foreign_key_rows_match_the_verified_oracle() {
        let (kv, schema) = oracle_catalog();
        let parent = oid_of(schema.parent);
        let all = rows(&kv, "pg_constraint", 0).expect("rows");
        let cases = [
            (
                "cc_a_fkey",
                OracleFacts {
                    condeferrable: Datum::Bool(false),
                    condeferred: Datum::Bool(false),
                    convalidated: Datum::Bool(true),
                    confrelid: parent.clone(),
                    confupdtype: text("a"),
                    confdeltype: text("a"),
                    confmatchtype: text("s"),
                    conkey: int2s(&[1]),
                    confkey: int2s(&[1]),
                    confdelsetcols: Datum::Null,
                },
            ),
            (
                "cc_def",
                OracleFacts {
                    condeferrable: Datum::Bool(true),
                    condeferred: Datum::Bool(true),
                    convalidated: Datum::Bool(true),
                    confrelid: parent.clone(),
                    confupdtype: text("a"),
                    confdeltype: text("d"),
                    confmatchtype: text("s"),
                    conkey: int2s(&[3]),
                    confkey: int2s(&[2]),
                    confdelsetcols: Datum::Null,
                },
            ),
            (
                "cc_full",
                OracleFacts {
                    condeferrable: Datum::Bool(false),
                    condeferred: Datum::Bool(false),
                    convalidated: Datum::Bool(true),
                    confrelid: parent.clone(),
                    confupdtype: text("c"),
                    confdeltype: text("n"),
                    confmatchtype: text("f"),
                    conkey: int2s(&[2]),
                    confkey: int2s(&[1]),
                    confdelsetcols: Datum::Null,
                },
            ),
            (
                "cc_nv",
                OracleFacts {
                    condeferrable: Datum::Bool(false),
                    condeferred: Datum::Bool(false),
                    convalidated: Datum::Bool(false),
                    confrelid: parent,
                    confupdtype: text("a"),
                    confdeltype: text("r"),
                    confmatchtype: text("s"),
                    conkey: int2s(&[1]),
                    confkey: int2s(&[1]),
                    confdelsetcols: Datum::Null,
                },
            ),
        ];
        for (name, expected) in cases {
            let row = row_named("pg_constraint", &all, "conname", name);
            assert!(oracle_facts(&row) == expected, "{name}");
        }
    }

    /// Every referential-action code and both match codes, one foreign key per
    /// combination.
    #[test]
    fn pg_constraint_encodes_every_referential_action_and_match_code() {
        let actions = [
            (ReferentialAction::NoAction, "a"),
            (ReferentialAction::Restrict, "r"),
            (ReferentialAction::Cascade, "c"),
            (ReferentialAction::SetNull, "n"),
            (ReferentialAction::SetDefault, "d"),
        ];
        let matches = [(MatchType::Simple, "s"), (MatchType::Full, "f")];
        let kv = MemKv::default();
        let schema = oracle_schema(&kv);
        for (row, (action, code)) in actions.into_iter().enumerate() {
            for (column, (match_type, match_code)) in matches.into_iter().enumerate() {
                let id = ForeignKeyId::try_from(row * matches.len() + column + 1)
                    .expect("foreign key id");
                add_foreign_key(
                    &kv,
                    &ForeignKey {
                        on_update: action,
                        on_delete: action,
                        match_type,
                        ..base_foreign_key(&schema, id, &format!("fk_{code}_{match_code}"))
                    },
                );
            }
        }

        let all = rows(&kv, "pg_constraint", 0).expect("rows");

        for (_, code) in actions {
            for (_, match_code) in matches {
                let name = format!("fk_{code}_{match_code}");
                let row = row_named("pg_constraint", &all, "conname", &name);
                let found = [
                    field("pg_constraint", &row, "confupdtype"),
                    field("pg_constraint", &row, "confdeltype"),
                    field("pg_constraint", &row, "confmatchtype"),
                ];
                assert!(
                    found == [text(code), text(code), text(match_code)],
                    "{name}"
                );
            }
        }
    }

    /// PostgreSQL stores `conkey` and `confkey` in the order the FK clause
    /// wrote them, paired positionally — neither sorted nor permuted into the
    /// referenced index's own column order.
    #[test]
    fn pg_constraint_keeps_composite_key_columns_in_written_order() {
        let kv = permuted_catalog();

        let all = rows(&kv, "pg_constraint", 0).expect("rows");
        let row = row_named("pg_constraint", &all, "conname", "cperm_b_a_fkey");

        assert!(field("pg_constraint", &row, "conkey") == int2s(&[2, 1]));
        assert!(field("pg_constraint", &row, "confkey") == int2s(&[2, 1]));
    }

    /// `confdelsetcols` holds the written `ON DELETE SET … (cols)` list, in
    /// written order, and is NULL when no list was written — PostgreSQL does
    /// not fill it in with a copy of `conkey`.
    #[test]
    fn pg_constraint_records_the_on_delete_set_column_list_only_when_written() {
        let kv = MemKv::default();
        let schema = oracle_schema(&kv);
        let composite = ForeignKey {
            columns: vec!["b".to_string(), "c".to_string()],
            referenced_columns: vec!["id".to_string(), "k".to_string()],
            referenced_index: "pp_id_k_key".to_string(),
            referenced_index_id: schema.composite,
            on_delete: ReferentialAction::SetNull,
            ..base_foreign_key(&schema, 1, "cc_setcols")
        };
        add_foreign_key(
            &kv,
            &ForeignKey {
                set_columns: vec!["c".to_string(), "b".to_string()],
                ..composite.clone()
            },
        );
        add_foreign_key(
            &kv,
            &ForeignKey {
                id: 2,
                name: "cc_no_setcols".to_string(),
                ..composite
            },
        );

        let all = rows(&kv, "pg_constraint", 0).expect("rows");

        let with = row_named("pg_constraint", &all, "conname", "cc_setcols");
        assert!(field("pg_constraint", &with, "confdelsetcols") == int2s(&[3, 2]));
        let without = row_named("pg_constraint", &all, "conname", "cc_no_setcols");
        assert!(field("pg_constraint", &without, "confdelsetcols") == Datum::Null);
    }

    #[test]
    fn foreign_key_constraint_oids_are_distinct_and_in_their_own_band() {
        let (kv, _) = oracle_catalog();

        let oids = foreign_key_constraint_oids(&kv).expect("oids");

        assert!(oids.len() == 4);
        // Constraint names are unique per relation, not per catalog, so the key
        // carries the child relation.
        assert!(oids.contains_key("cc.cc_a_fkey"));
        let band = FOREIGN_KEY_OID_BASE..FOREIGN_KEY_OID_BASE + OID_BAND_WIDTH;
        for (key, oid) in &oids {
            assert!(band.contains(oid), "{key} oid {oid} is out of band");
        }
        let distinct = oids.values().collect::<std::collections::BTreeSet<_>>();
        assert!(distinct.len() == oids.len());
    }

    /// A foreign key's oid is its stored id placed in the foreign-key band, so
    /// the band has to be disjoint from the ones the other constraint kinds
    /// report in — and an id that would leave it is refused rather than
    /// answered inside a neighbour's.
    #[test]
    fn a_foreign_key_oid_stays_inside_the_foreign_key_band() {
        let band = FOREIGN_KEY_OID_BASE..FOREIGN_KEY_OID_BASE + OID_BAND_WIDTH;
        for neighbour in [CHECK_OID_BASE, NOT_NULL_OID_BASE, CONSTRAINT_OID_BASE] {
            assert!(!band.contains(&neighbour));
            assert!(!(neighbour..neighbour + OID_BAND_WIDTH).contains(&FOREIGN_KEY_OID_BASE));
        }

        let last = OID_BAND_WIDTH.unsigned_abs() - 1;
        assert!(foreign_key_oid(1).expect("first id") == FOREIGN_KEY_OID_BASE + 1);
        assert!(
            foreign_key_oid(last).expect("last id") == FOREIGN_KEY_OID_BASE + OID_BAND_WIDTH - 1
        );
        assert!(foreign_key_oid(last + 1).is_err());
    }

    /// An index id is placed in its band the same way, and the bands are
    /// adjacent — `INDEX_OID_BASE` borders the view band and
    /// `CONSTRAINT_OID_BASE` borders the `CHECK` band. Past roughly ten
    /// thousand indexes an unbounded add would answer inside a neighbour's
    /// band, so two distinct objects would report one `pg_class` or
    /// `pg_constraint` oid with nothing raised.
    #[test]
    fn an_index_oid_stays_inside_its_band() {
        let last = OID_BAND_WIDTH.unsigned_abs() - 1;

        struct Case {
            what: &'static str,
            oid: fn(u32) -> Result<i32, ExecError>,
            base: i32,
            neighbour: i32,
        }
        let cases = [
            Case {
                what: "index relation",
                oid: index_relation_oid,
                base: INDEX_OID_BASE,
                neighbour: VIEW_OID_BASE,
            },
            Case {
                what: "index constraint",
                oid: index_constraint_oid,
                base: CONSTRAINT_OID_BASE,
                neighbour: CHECK_OID_BASE,
            },
        ];

        for case in cases {
            assert!(
                (case.oid)(0).expect("first id") == case.base,
                "{} first id",
                case.what
            );
            let highest = (case.oid)(last).expect("last id");
            assert!(
                highest == case.base + OID_BAND_WIDTH - 1,
                "{} last id",
                case.what
            );
            // The bases sit further apart than the band is wide, so the bound
            // refuses before an id could reach the neighbour rather than at the
            // exact point of collision. Assert the separation rather than
            // adjacency: the band must not reach the neighbour's base, and an
            // id past the band must be refused even though the first few would
            // still land in the slack between them.
            assert!(
                !(case.base..case.base + OID_BAND_WIDTH).contains(&case.neighbour),
                "{} band reaches its neighbour",
                case.what
            );
            assert!(highest < case.neighbour, "{} overlaps", case.what);
            assert!((case.oid)(last + 1).is_err(), "{} past the band", case.what);
        }
    }

    /// A one-column `c` table, `NOT NULL` and carrying a `CHECK` named `ck`.
    fn add_checked_table(kv: &MemKv, name: &RelationName) -> TableId {
        let column = Column {
            not_null: true,
            ..int4("c")
        };
        let (id, ops) = crabka_pgcatalog::create_table_with_options_ops(
            kv,
            name,
            vec![column],
            crabka_pgcatalog::TableOptions::default(),
            vec![crabka_pgcatalog::CheckConstraint {
                name: "ck".to_string(),
                expr: "c > 0".to_string(),
                validated: true,
            }],
            crabka_pgcatalog::TableIdSource::Counter,
        )
        .expect("table ops");
        kv.write_batch(&ops).expect("write table");
        id
    }

    /// Two relations whose schema and name flatten to the same dotted text:
    /// `"s.t"` created in `public`, and `t` created in schema `s`. Each carries
    /// a `CHECK` named `ck`, a `NOT NULL` on `c` and a foreign key named `fk`
    /// into `public.p`.
    fn dotted_name_catalog() -> MemKv {
        let kv = MemKv::default();
        let ops = crabka_pgcatalog::create_schema_ops(&kv, "s", "postgres").expect("schema ops");
        kv.write_batch(&ops).expect("write schema");
        let referent = RelationName::public("p");
        let parent = crabka_pgcatalog::create_table(&kv, &referent, vec![int4("id")]).expect("p");
        let unique = add_index(
            &kv,
            &referent,
            "p_id_key",
            &["id"],
            Some(IndexConstraint::Unique),
        );
        for (id, table) in [
            (1, RelationName::public("s.t")),
            (2, RelationName::new("s", "t")),
        ] {
            let table_id = add_checked_table(&kv, &table);
            add_foreign_key(
                &kv,
                &ForeignKey {
                    id,
                    name: "fk".to_string(),
                    table,
                    table_id,
                    columns: vec!["c".to_string()],
                    referenced_table: referent.clone(),
                    referenced_table_id: parent,
                    referenced_columns: vec!["id".to_string()],
                    referenced_index: "p_id_key".to_string(),
                    referenced_index_id: unique,
                    match_type: MatchType::Simple,
                    on_delete: ReferentialAction::NoAction,
                    on_update: ReferentialAction::NoAction,
                    set_columns: Vec::new(),
                    deferrable: false,
                    initially_deferred: false,
                    validated: true,
                },
            );
        }
        kv
    }

    /// A dot only reaches an identifier through quoting, and that is where a
    /// flattened `<table>.<constraint>` key stops being injective: `"s.t"` in
    /// `public` and `t` in schema `s` are two relations, so a same-named
    /// constraint on each is two constraints. [`banded_oids`] deduplicates its
    /// input, so one flattened key would collapse the pair into a single entry
    /// and report one oid for both `pg_constraint` rows — which is what any
    /// join on that oid then mis-associates.
    #[test]
    fn constraint_oids_separate_a_dotted_relation_from_a_qualified_one() {
        let kv = dotted_name_catalog();

        let checks = check_constraint_oids(&kv).expect("check oids");
        let not_nulls = not_null_constraint_oids(&kv).expect("not null oids");
        let all = rows(&kv, "pg_constraint", 0).expect("rows");

        assert!(checks.len() == 2, "a CHECK constraint lost its own key");
        assert!(
            not_nulls.len() == 2,
            "a NOT NULL constraint lost its own key"
        );
        // Covers the foreign keys too, whose oids come from their stored ids.
        let oids = all
            .iter()
            .map(|row| match field("pg_constraint", row, "oid") {
                Datum::Int4(oid) => oid,
                other => panic!("pg_constraint.oid is {other:?}"),
            })
            .collect::<Vec<_>>();
        let distinct = oids.iter().collect::<std::collections::BTreeSet<_>>();
        assert!(distinct.len() == oids.len(), "two constraints share an oid");
    }

    /// `match_option` uses the SQL standard's `NONE` for MATCH SIMPLE, and the
    /// rules are spelled out rather than coded.
    #[test]
    fn referential_constraints_spell_out_the_oracle_rules() {
        const VIEW: &str = "information_schema.referential_constraints";
        let (kv, _) = oracle_catalog();
        let all = rows(&kv, VIEW, 0).expect("rows");
        let cases = [
            ("cc_a_fkey", "pp_pkey", "NONE", "NO ACTION", "NO ACTION"),
            ("cc_def", "pp_k_key", "NONE", "NO ACTION", "SET DEFAULT"),
            ("cc_full", "pp_pkey", "FULL", "CASCADE", "SET NULL"),
            ("cc_nv", "pp_pkey", "NONE", "NO ACTION", "RESTRICT"),
        ];
        for (name, unique, match_option, update_rule, delete_rule) in cases {
            let row = row_named(VIEW, &all, "constraint_name", name);
            assert!(
                row == vec![
                    catalog_name(),
                    text("public"),
                    text(name),
                    catalog_name(),
                    text("public"),
                    text(unique),
                    text(match_option),
                    text(update_rule),
                    text(delete_rule),
                ],
                "{name}"
            );
        }
    }

    /// A foreign key may target a bare `CREATE UNIQUE INDEX`, which carries no
    /// `pg_constraint` row — PostgreSQL's view LEFT JOINs to find one, so the
    /// whole `unique_constraint_*` triple comes back NULL.
    #[test]
    fn referential_constraints_null_the_unique_constraint_for_a_bare_unique_index() {
        const VIEW: &str = "information_schema.referential_constraints";
        let kv = MemKv::default();
        let schema = oracle_schema(&kv);
        add_foreign_key(
            &kv,
            &ForeignKey {
                columns: vec!["c".to_string()],
                referenced_columns: vec!["m".to_string()],
                referenced_index: "pp_m_idx".to_string(),
                referenced_index_id: schema.bare,
                ..base_foreign_key(&schema, 1, "cc_bare")
            },
        );

        let all = rows(&kv, VIEW, 0).expect("rows");

        assert!(
            row_named(VIEW, &all, "constraint_name", "cc_bare")
                == vec![
                    catalog_name(),
                    text("public"),
                    text("cc_bare"),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    text("NONE"),
                    text("NO ACTION"),
                    text("NO ACTION"),
                ]
        );
    }

    /// `table_constraints` gains a `FOREIGN KEY` row per foreign key, and it is
    /// the one constraint kind that can report anything but `NO`/`NO`.
    #[test]
    fn table_constraints_report_real_deferrability_for_foreign_keys() {
        const VIEW: &str = "information_schema.table_constraints";
        let (kv, _) = oracle_catalog();
        let all = rows(&kv, VIEW, 0).expect("rows");
        let cases = [("cc_a_fkey", "NO", "NO"), ("cc_def", "YES", "YES")];
        for (name, deferrable, deferred) in cases {
            let row = row_named(VIEW, &all, "constraint_name", name);
            assert!(
                row == vec![
                    catalog_name(),
                    text("public"),
                    text(name),
                    catalog_name(),
                    text("public"),
                    text("cc"),
                    text("FOREIGN KEY"),
                    text(deferrable),
                    text(deferred),
                    text("YES"),
                    Datum::Null,
                ],
                "{name}"
            );
        }
        // Lifting the two flags to parameters must not have moved the other
        // constraint kinds off `NO`/`NO`.
        let pkey = row_named(VIEW, &all, "constraint_name", "pp_pkey");
        assert!(field(VIEW, &pkey, "is_deferrable") == text("NO"));
        assert!(field(VIEW, &pkey, "initially_deferred") == text("NO"));
    }

    /// `ordinal_position` follows the FK clause while
    /// `position_in_unique_constraint` follows the referenced index, so the
    /// permuted composite reports 1→2 and 2→1.
    #[test]
    fn key_column_usage_lists_referencing_columns_with_their_index_positions() {
        const VIEW: &str = "information_schema.key_column_usage";
        let kv = permuted_catalog();

        let all = rows(&kv, VIEW, 0).expect("rows");

        assert!(
            rows_named(VIEW, &all, "constraint_name", "cperm_b_a_fkey")
                == vec![
                    vec![
                        catalog_name(),
                        text("public"),
                        text("cperm_b_a_fkey"),
                        catalog_name(),
                        text("public"),
                        text("cperm"),
                        text("b"),
                        int(1),
                        int(2),
                    ],
                    vec![
                        catalog_name(),
                        text("public"),
                        text("cperm_b_a_fkey"),
                        catalog_name(),
                        text("public"),
                        text("cperm"),
                        text("a"),
                        int(2),
                        int(1),
                    ],
                ]
        );
    }

    /// `constraint_column_usage` attributes a foreign key to the columns it
    /// *references*, on the parent relation — the mirror of `key_column_usage`.
    #[test]
    fn constraint_column_usage_names_the_referenced_table_for_a_foreign_key() {
        const VIEW: &str = "information_schema.constraint_column_usage";
        let (kv, _) = oracle_catalog();

        let all = rows(&kv, VIEW, 0).expect("rows");

        assert!(
            rows_named(VIEW, &all, "constraint_name", "cc_def")
                == vec![vec![
                    catalog_name(),
                    text("public"),
                    text("pp"),
                    text("k"),
                    catalog_name(),
                    text("public"),
                    text("cc_def"),
                ]]
        );
    }
}
