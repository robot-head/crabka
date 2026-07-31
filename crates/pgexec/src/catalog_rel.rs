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

use crabka_pgcatalog::{Column, IndexConstraint};
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

/// Assign every name in `names` a distinct oid inside the band starting at
/// `base`. The slot is a hash of the name, so an object keeps its oid when
/// unrelated objects are created or dropped; a collision probes forward, which
/// makes the whole assignment a pure function of the (sorted) name set.
fn banded_oids(base: i32, names: &[String]) -> BTreeMap<String, i32> {
    let mut taken = BTreeMap::new();
    let mut used = std::collections::BTreeSet::new();
    let mut sorted: Vec<&String> = names.iter().collect();
    sorted.sort();
    sorted.dedup();
    for name in sorted {
        let mut slot = i32::try_from(fnv1a(name) % OID_BAND_WIDTH.unsigned_abs()).unwrap_or(0);
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

/// `pg_class` oids of every view, keyed by view name.
pub(crate) fn view_oids(kv: &dyn Kv) -> Result<BTreeMap<String, i32>, ExecError> {
    let names = crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| view.name)
        .collect::<Vec<_>>();
    Ok(banded_oids(VIEW_OID_BASE, &names))
}

/// `pg_class` oids of every sequence, keyed by sequence name.
pub(crate) fn sequence_oids(kv: &dyn Kv) -> Result<BTreeMap<String, i32>, ExecError> {
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

/// `pg_constraint` oids of every `CHECK` constraint, keyed `<table>.<name>`.
pub(crate) fn check_constraint_oids(kv: &dyn Kv) -> Result<BTreeMap<String, i32>, ExecError> {
    let keys = crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .flat_map(|table| {
            table
                .checks
                .iter()
                .map(|check| format!("{}.{}", table.name, check.name))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(banded_oids(CHECK_OID_BASE, &keys))
}

/// `pg_constraint` oids of every `NOT NULL` constraint, keyed `<table>.<column>`.
pub(crate) fn not_null_constraint_oids(kv: &dyn Kv) -> Result<BTreeMap<String, i32>, ExecError> {
    let keys = crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .flat_map(|table| {
            table
                .columns
                .iter()
                .filter(|column| column.not_null)
                .map(|column| format!("{}.{}", table.name, column.name))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(banded_oids(NOT_NULL_OID_BASE, &keys))
}

/// The `pg_constraint` oid of the constraint an index backs.
pub(crate) fn index_constraint_oid(index_id: u32) -> Result<i32, ExecError> {
    i32::try_from(index_id)
        .ok()
        .and_then(|id| CONSTRAINT_OID_BASE.checked_add(id))
        .ok_or_else(|| ExecError::Unsupported("constraint oid exceeds int4 range".into()))
}

/// The `pg_class` oid of an index, mirroring `exec::catalog_index_oid` so both
/// sides of a `pg_index`/`pg_class` join agree.
pub(crate) fn index_relation_oid(index_id: u32) -> Result<i32, ExecError> {
    i32::try_from(index_id)
        .ok()
        .and_then(|id| INDEX_OID_BASE.checked_add(id))
        .ok_or_else(|| ExecError::Unsupported("index oid exceeds int4 range".into()))
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

/// The relation's rows.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn rows(kv: &dyn Kv, name: &str) -> Result<Vec<Vec<Datum>>, ExecError> {
    match name {
        "pg_am" => Ok(pg_am_rows()),
        "pg_language" => Ok(pg_language_rows()),
        "pg_proc" => crate::routine::pg_proc_rows(kv),
        "pg_attrdef" => pg_attrdef_rows(kv),
        "pg_authid" => pg_authid_rows(kv),
        "pg_collation" => Ok(pg_collation_rows()),
        "pg_constraint" => pg_constraint_rows(kv),
        "pg_database" => Ok(pg_database_rows()),
        "pg_description" => pg_description_rows(kv),
        "pg_indexes" => pg_indexes_rows(kv),
        "pg_rewrite" => pg_rewrite_rows(kv),
        "pg_sequence" => pg_sequence_rows(kv),
        "pg_stat_activity" => Ok(pg_stat_activity_rows()),
        "pg_tables" => pg_tables_rows(kv),
        "pg_tablespace" => Ok(pg_tablespace_rows()),
        "pg_views" => pg_views_rows(kv),
        _ => information_schema_rows(kv, name),
    }
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
        (405, "hash", "i"),
        (783, "gist", "i"),
        (2742, "gin", "i"),
        (4000, "spgist", "i"),
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
                text(&crate::catalog_fn::default_source_text(default, column.ty)),
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
fn pg_stat_activity_rows() -> Vec<Vec<Datum>> {
    vec![vec![
        int(DATABASE_OID),
        text(crate::exec::CURRENT_DATABASE),
        int(crate::catalog_fn::backend_pid()),
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
        if let Some(comment) = crabka_pgcatalog::get_comment(kv, "table", &table.name)? {
            rows.push(description_row(relid, 0, &comment));
        }
        for (idx, column) in table.columns.iter().enumerate() {
            let key = format!("{}.{}", table.name, column.name);
            if let Some(comment) = crabka_pgcatalog::get_comment(kv, "column", &key)? {
                let attnum = i32::try_from(idx + 1)
                    .map_err(|_| ExecError::Unsupported("attnum exceeds int4 range".into()))?;
                rows.push(description_row(relid, attnum, &comment));
            }
        }
    }
    let view_oids = view_oids(kv)?;
    for view in crabka_pgcatalog::list_views(kv)? {
        if let Some(comment) = crabka_pgcatalog::get_comment(kv, "view", &view.name)? {
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

/// Primary-key/unique constraints (each backed by an index) and `CHECK`
/// constraints. crabka has no foreign keys yet, so no `'f'` row is ever
/// produced — `\d`'s foreign-key section is correctly empty rather than absent.
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
        };
        let relid = i32::try_from(index.table_id)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?;
        rows.push(constraint_row(ConstraintRow {
            oid: index_constraint_oid(index.id)?,
            name: &index.name,
            contype,
            conrelid: relid,
            conindid: index_relation_oid(index.id)?,
            conkey: Some(conkey),
            conbin: Datum::Null,
            validated: true,
        }));
    }
    rows.extend(check_constraint_rows(kv)?);
    Ok(rows)
}

/// `CHECK` constraints (`contype = 'c'`) and the `NOT NULL` constraints
/// PostgreSQL 18 records alongside them (`contype = 'n'`, named
/// `<table>_<column>_not_null`).
fn check_constraint_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let check_oids = check_constraint_oids(kv)?;
    let not_null_oids = not_null_constraint_oids(kv)?;
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(kv)? {
        let relid = i32::try_from(table.id)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?;
        for check in &table.checks {
            let key = format!("{}.{}", table.name, check.name);
            rows.push(constraint_row(ConstraintRow {
                oid: check_oids.get(&key).copied().unwrap_or(0),
                name: &check.name,
                contype: "c",
                conrelid: relid,
                conindid: 0,
                conkey: None,
                conbin: text(&check.expr),
                validated: check.validated,
            }));
        }
        for (idx, column) in table.columns.iter().enumerate() {
            if !column.not_null {
                continue;
            }
            let key = format!("{}.{}", table.name, column.name);
            let attnum = i16::try_from(idx + 1)
                .map_err(|_| ExecError::Unsupported("attnum exceeds int2 range".into()))?;
            rows.push(constraint_row(ConstraintRow {
                oid: not_null_oids.get(&key).copied().unwrap_or(0),
                name: &format!("{}_{}_not_null", table.name, column.name),
                contype: "n",
                conrelid: relid,
                conindid: 0,
                conkey: Some(vec![Datum::Int2(attnum)]),
                conbin: Datum::Null,
                validated: true,
            }));
        }
    }
    Ok(rows)
}

/// The `pg_constraint` fields that vary by constraint kind; the rest of the
/// wide tuple is the same for every row crabka produces.
struct ConstraintRow<'a> {
    oid: i32,
    name: &'a str,
    contype: &'a str,
    conrelid: i32,
    conindid: i32,
    conkey: Option<Vec<Datum>>,
    conbin: Datum,
    validated: bool,
}

fn constraint_row(row: ConstraintRow<'_>) -> Vec<Datum> {
    let conkey = row.conkey.map_or(Datum::Null, |elems| {
        Datum::Array(crabka_pgtypes::ArrayValue::new(ElemType::Int2, elems))
    });
    vec![
        int(row.oid),
        text(row.name),
        int(crate::exec::PUBLIC_NAMESPACE_OID),
        text(row.contype),
        Datum::Bool(false),
        Datum::Bool(false),
        Datum::Bool(true),
        Datum::Bool(row.validated),
        int(row.conrelid),
        int(0),
        int(row.conindid),
        int(0),
        int(0),
        text(" "),
        text(" "),
        text(" "),
        Datum::Bool(true),
        small(0),
        Datum::Bool(false),
        Datum::Bool(false),
        conkey,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Null,
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
                text("public"),
                text(&index.table),
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
                text("public"),
                text(&table.name),
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
                text("public"),
                text(&view.name),
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
        "information_schema.views" => information_schema_view_rows(kv),
        "information_schema.enabled_roles" => enabled_role_rows(kv),
        "information_schema.applicable_roles" => Ok(Vec::new()),
        "information_schema.sequences" => sequence_view_rows(kv),
        "information_schema.table_privileges" => table_privilege_rows(kv),
        "information_schema.column_privileges" => Ok(Vec::new()),
        // `referential_constraints` needs foreign keys, `routines`/`parameters`
        // need user-defined routines; crabka has neither object kind yet, so
        // both are correctly empty rather than absent.
        _ => Ok(Vec::new()),
    }
}

fn catalog_name() -> Datum {
    text(crate::exec::CURRENT_DATABASE)
}

/// A constraint's `information_schema` identity: catalog, schema, name.
fn constraint_identity(name: &str) -> [Datum; 3] {
    [catalog_name(), text("public"), text(name)]
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
        };
        rows.push(table_constraint_row(
            &index.name,
            &index.table,
            constraint_type,
        ));
    }
    for table in crabka_pgcatalog::list_tables(kv)? {
        for check in &table.checks {
            rows.push(table_constraint_row(&check.name, &table.name, "CHECK"));
        }
    }
    Ok(rows)
}

fn table_constraint_row(name: &str, table: &str, constraint_type: &str) -> Vec<Datum> {
    let mut row = constraint_identity(name).to_vec();
    row.extend([catalog_name(), text("public"), text(table)]);
    row.extend([
        text(constraint_type),
        text("NO"),
        text("NO"),
        text("YES"),
        if constraint_type == "UNIQUE" {
            text("YES")
        } else {
            Datum::Null
        },
    ]);
    row
}

fn key_column_usage_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if index.constraint.is_none() {
            continue;
        }
        for (position, column) in index.columns.iter().enumerate() {
            let ordinal = i32::try_from(position + 1)
                .map_err(|_| ExecError::Unsupported("ordinal exceeds int4 range".into()))?;
            let mut row = constraint_identity(&index.name).to_vec();
            row.extend([catalog_name(), text("public"), text(&index.table)]);
            row.extend([text(column), int(ordinal), Datum::Null]);
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
            rows.push(column_usage_row(&index.table, column, &index.name));
        }
    }
    // PostgreSQL 18 records `NOT NULL` in `pg_constraint`, so it shows up here
    // too, one row per constrained column.
    for table in crabka_pgcatalog::list_tables(kv)? {
        for column in table.columns.iter().filter(|column| column.not_null) {
            let name = format!("{}_{}_not_null", table.name, column.name);
            rows.push(column_usage_row(&table.name, &column.name, &name));
        }
    }
    Ok(rows)
}

fn column_usage_row(table: &str, column: &str, constraint: &str) -> Vec<Datum> {
    let mut row = vec![catalog_name(), text("public"), text(table), text(column)];
    row.extend(constraint_identity(constraint));
    row
}

fn information_schema_view_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| {
            let updatable = crate::catalog_fn::view_is_auto_updatable(&view);
            let flag = if updatable { "YES" } else { "NO" };
            vec![
                catalog_name(),
                text("public"),
                text(&view.name),
                text(&crate::catalog_fn::view_definition_text(&view, false)),
                text("NONE"),
                text(flag),
                text(flag),
                text("NO"),
                text("NO"),
                text("NO"),
            ]
        })
        .collect())
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
            vec![
                catalog_name(),
                text("public"),
                text(&name),
                text("bigint"),
                int(64),
                int(2),
                int(0),
                text(&sequence.start.to_string()),
                text(&sequence.min.to_string()),
                text(&sequence.max.to_string()),
                text(&sequence.increment.to_string()),
                text(if sequence.cycle { "YES" } else { "NO" }),
            ]
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

fn privilege_row(grantee: &str, table: &str, privilege: &str, owned: bool) -> Vec<Datum> {
    vec![
        text(crate::catalog_fn::OBJECT_OWNER),
        text(grantee),
        catalog_name(),
        text("public"),
        text(table),
        text(privilege),
        text(if owned { "YES" } else { "NO" }),
        text(if owned && privilege == "SELECT" {
            "YES"
        } else {
            "NO"
        }),
    ]
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{banded_oids, catalog_relation, columns, relation_names, relation_oid};

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
        let first = banded_oids(1_000, &["a".into(), "b".into()]);
        let second = banded_oids(1_000, &["a".into(), "b".into(), "c".into()]);
        assert!(first["a"] == second["a"]);
        assert!(first["b"] == second["b"]);
    }

    #[test]
    fn banded_oids_are_distinct_for_distinct_names() {
        let names = (0..200).map(|n| format!("t{n}")).collect::<Vec<_>>();
        let assigned = banded_oids(1_000, &names);
        let distinct = assigned.values().collect::<std::collections::BTreeSet<_>>();
        assert!(distinct.len() == names.len());
    }
}
