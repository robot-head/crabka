//! DDL and catalog code carved out of `exec`.

use std::collections::BTreeMap;

use super::*;

pub(crate) fn cols(defs: &[(&str, ColumnType)]) -> Vec<Column> {
    defs.iter()
        .map(|(name, ty)| Column::new(*name, *ty))
        .collect()
}

pub(crate) fn pg_class_columns() -> Vec<Column> {
    use ColumnType::{Array, Bool, Float4, Int2, Int4, Int8, Text};
    cols(&[
        ("oid", Int4),
        ("relname", Text),
        ("relnamespace", Int4),
        ("reltype", Int4),
        ("reloftype", Int4),
        ("relowner", Int4),
        ("relam", Int4),
        ("relfilenode", Int4),
        ("reltablespace", Int4),
        ("relpages", Int4),
        ("reltuples", Float4),
        ("relallvisible", Int4),
        ("relallfrozen", Int4),
        ("reltoastrelid", Int4),
        ("relhasindex", Bool),
        ("relisshared", Bool),
        ("relpersistence", Text),
        ("relkind", Text),
        ("relnatts", Int2),
        ("relchecks", Int2),
        ("relhasrules", Bool),
        ("relhastriggers", Bool),
        ("relhassubclass", Bool),
        ("relrowsecurity", Bool),
        ("relforcerowsecurity", Bool),
        ("relispopulated", Bool),
        ("relreplident", Text),
        ("relispartition", Bool),
        ("relrewrite", Int4),
        ("relfrozenxid", Int8),
        ("relminmxid", Int8),
        ("relacl", Array(crabka_pgtypes::ElemType::Text)),
        ("reloptions", Array(crabka_pgtypes::ElemType::Text)),
        ("relpartbound", Text),
    ])
}

pub(crate) fn pg_attribute_columns() -> Vec<Column> {
    use ColumnType::{Array, Bool, Int2, Int4, Text};
    cols(&[
        ("attrelid", Int4),
        ("attname", Text),
        ("atttypid", Int4),
        ("attlen", Int2),
        ("attnum", Int2),
        ("atttypmod", Int4),
        ("attndims", Int2),
        ("attbyval", Bool),
        ("attalign", ColumnType::InternalChar),
        ("attstorage", ColumnType::InternalChar),
        ("attcompression", ColumnType::InternalChar),
        ("attnotnull", Bool),
        ("atthasdef", Bool),
        ("atthasmissing", Bool),
        ("attidentity", ColumnType::InternalChar),
        ("attgenerated", ColumnType::InternalChar),
        ("attisdropped", Bool),
        ("attislocal", Bool),
        ("attinhcount", Int2),
        ("attcollation", Int4),
        ("attstattarget", Int2),
        ("attacl", Array(crabka_pgtypes::ElemType::Text)),
        ("attoptions", Array(crabka_pgtypes::ElemType::Text)),
        ("attfdwoptions", Array(crabka_pgtypes::ElemType::Text)),
        ("attmissingval", Array(crabka_pgtypes::ElemType::Text)),
    ])
}

pub(crate) fn virtual_catalog_rows(
    catalog_kv: &dyn Kv,
    name: &str,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    match name {
        "pg_namespace" => pg_namespace_rows(catalog_kv),
        "pg_class" => pg_class_rows(catalog_kv),
        "pg_attribute" => pg_attribute_rows(catalog_kv),
        "pg_type" => pg_type_rows(catalog_kv),
        "pg_ts_config" => text_search_catalog_rows(
            catalog_kv,
            crabka_pgparser::ast::TextSearchObjectKind::Configuration,
        ),
        "pg_ts_dict" => text_search_catalog_rows(
            catalog_kv,
            crabka_pgparser::ast::TextSearchObjectKind::Dictionary,
        ),
        "pg_range" => pg_range_rows(catalog_kv),
        "pg_index" => pg_index_rows(catalog_kv),
        "pg_settings" => pg_settings_rows(),
        "pg_prepared_statements" => pg_prepared_statement_rows(),
        "pg_roles" => pg_roles_rows(catalog_kv),
        "pg_user" => pg_user_rows(catalog_kv),
        "information_schema.schemata" => {
            information_schema_schemata_rows(catalog_kv, ctx.database())
        }
        "information_schema.tables" => {
            information_schema_tables_rows(catalog_kv, ctx.database(), ctx.backend_pid)
        }
        "information_schema.columns" => {
            information_schema_columns_rows(catalog_kv, ctx.backend_pid)
        }
        "information_schema.triggers" => {
            information_schema_trigger_rows(catalog_kv, ctx.database())
        }
        "information_schema.triggered_update_columns" => {
            information_schema_triggered_update_column_rows(catalog_kv, ctx.database())
        }
        "pg_inherits" => pg_inherits_rows(catalog_kv),
        "pg_partitioned_table" => pg_partitioned_table_rows(catalog_kv),
        _ => crate::catalog_rel::rows(
            catalog_kv,
            name,
            crate::catalog_rel::SessionIdent {
                database: ctx.database(),
                backend_pid: ctx.backend_pid,
                style: ctx.output_style(),
            },
        ),
    }
}

/// `pg_inherits`: one row per inheritance child or partition, naming its direct
/// parent.
///
/// Both `inhrelid` and `inhparent` are `pg_class` oids, so a parent's table id
/// goes through the same [`crate::catalog_rel::table_relation_oid`] derivation
/// the child's does. Reading the parent's id out of the relation list already
/// in hand also spares a catalog `get` per parent.
///
/// A partition is always its parent's only inheritance step, so `inhseqno` is
/// 1 and `inhdetachpending` false. The concurrent-detach flag has no state to
/// report here, because detach is a single catalog batch.
///
/// # A parent name that resolves to nothing
///
/// `PostgreSQL` stores the parent as an oid, so a row whose parent is gone is
/// still a row: it prints the number it holds and every join to `pg_class`
/// simply misses. Measured on 18.4, with `inhparent` hand-set to an oid no
/// relation carries, `SELECT * FROM pg_inherits` prints that number,
/// `inhparent::regclass` renders it as digits rather than erroring, a
/// `LEFT JOIN pg_class` yields NULL, and `\d` on the child, on the ex-parent
/// and on an unrelated relation is unaffected.
///
/// crabka stores the parent as a *name*, and a name that resolves to nothing
/// has no oid to print. It gets [`UNRESOLVED_PARENT_OID`], which is the one
/// value guaranteed to resolve to nothing, so every join behaves as it does
/// against `PostgreSQL`'s dangling oid.
///
/// The row is kept rather than dropped, and that is the whole point of the
/// treatment. This projection used to raise `UndefinedTable` for the *whole*
/// statement, so one stale key took `SELECT * FROM pg_inherits` — and so psql's
/// `\d` on every relation in the database — down with it. Dropping the row
/// instead would trade that for the opposite failure: the catalog would look
/// healthy and the missing link would be invisible. One odd row is what a
/// catalog table owes.
pub(crate) fn pg_inherits_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let tables = crabka_pgcatalog::list_tables(catalog_kv)?;
    let table_ids = tables
        .iter()
        .map(|table| (&table.name, table.id))
        .collect::<std::collections::HashMap<_, _>>();
    let mut rows = Vec::new();
    for table in &tables {
        let mut parents = crate::inheritance::parents_of(catalog_kv, &table.name)?;
        if let Some((parent, _)) = crate::partition::parent_of(catalog_kv, &table.name)? {
            parents.push(parent);
        }
        for (index, parent) in parents.into_iter().enumerate() {
            let parent_oid = table_ids
                .get(&parent)
                .map_or(Ok(UNRESOLVED_PARENT_OID), |id| {
                    crate::catalog_rel::table_relation_oid(*id)
                })?;
            rows.push(vec![
                int(crate::catalog_rel::table_relation_oid(table.id)?),
                int(parent_oid),
                int(i32::try_from(index + 1).unwrap_or(i32::MAX)),
                Datum::Bool(false),
            ]);
        }
    }
    Ok(rows)
}

/// The `inhparent` of a `pg_inherits` row whose stored parent name resolves to
/// no relation.
///
/// `InvalidOid` — `PostgreSQL`'s own "no such object" oid, and outside every
/// oid band [`crate::catalog_rel`] hands out, so it can never collide with a
/// live relation and make the row read as a link to the wrong table.
pub(crate) const UNRESOLVED_PARENT_OID: i32 = 0;

/// `pg_partitioned_table`: one row per partitioned parent.
pub(crate) fn pg_partitioned_table_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        let Some(scheme) = crate::partition::scheme_of(catalog_kv, &table.name)? else {
            continue;
        };
        let natts = i16::try_from(scheme.keys.len())
            .map_err(|_| ExecError::Unsupported("partnatts exceeds int2 range".into()))?;
        // `partattrs` is an int2vector, printed as a space-separated list of
        // one-based attribute numbers. Crabka compacts the column list on `DROP
        // COLUMN`, so an attribute number is the column's position *now* and is
        // derived here rather than stored.
        let attrs = crate::partition::key_ordinals(&scheme, &table.columns)?
            .into_iter()
            .map(|ordinal| (ordinal + 1).to_string())
            .collect::<Vec<_>>()
            .join(" ");
        rows.push(vec![
            int(crate::catalog_rel::table_relation_oid(table.id)?),
            text(scheme.strategy.code()),
            Datum::Int2(natts),
            int(0),
            text(&attrs),
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]);
    }
    Ok(rows)
}

/// `pg_namespace`: one row per schema the catalog holds — nothing is added
/// here, so a schema appears exactly once and a dropped one not at all.
pub(crate) fn pg_namespace_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_schemas(catalog_kv)?
        .into_iter()
        .map(|schema| {
            vec![
                int(crate::catalog_rel::namespace_oid(&schema.name)),
                text(&schema.name),
                int(schema_owner_oid(&schema.owner)),
                Datum::Null,
            ]
        })
        .collect())
}

/// The `pg_authid.oid` a schema's owner projects as. `public` belongs to the
/// implicit `pg_database_owner` role; every other schema projects the bootstrap
/// superuser, because trust auth makes every session that user and crabka has
/// no ownership model to distinguish them by.
pub(crate) fn schema_owner_oid(owner: &str) -> i32 {
    if owner == crabka_pgcatalog::PUBLIC_SCHEMA_OWNER {
        crate::catalog_fn::DATABASE_OWNER_ROLE_OID
    } else {
        crate::catalog_fn::BOOTSTRAP_ROLE_OID
    }
}

/// Every relation crabka has, in the `relkind` PostgreSQL would report: user
/// tables `r`, partitioned tables `p`, foreign tables `f`, materialized views
/// `m`, views `v`, sequences `S`, indexes `i`, and the virtual catalog
/// relations `v`. `psql`'s `\dt`/`\dv`/`\dm`/`\di`/`\ds` differ only in the
/// `relkind` they filter on, so all of them need this one list.
pub(crate) fn pg_class_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let triggered_relation_ids = crabka_pgcatalog::trigger::list_triggers(catalog_kv)?
        .into_iter()
        .map(|trigger| trigger.table_id)
        .collect::<std::collections::HashSet<_>>();
    let indexes = crabka_pgcatalog::list_indexes(catalog_kv)?;
    let indexed_table_ids = indexes
        .iter()
        .map(|index| index.table_id)
        .collect::<std::collections::BTreeSet<_>>();
    let role_oids = crate::catalog_rel::role_oids(catalog_kv)?;
    // Four scans, not four reads per relation: the stored `pg_class` statistics
    // are stored per relation and only stored relations ever have them.
    let relstats = crate::relstats::all(catalog_kv)?;
    // An index is owned by whoever owns the table it indexes, so the table
    // pass records what the index pass needs rather than reading the catalog
    // again per index.
    let mut table_owner_oids = std::collections::BTreeMap::new();
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        let partitioned = crate::partition::is_partitioned(catalog_kv, &table.name)?;
        let relkind = match (
            table.foreign.is_some(),
            table.materialized.is_some(),
            partitioned,
        ) {
            (true, _, _) => "f",
            (_, true, _) => "m",
            (false, false, true) => "p",
            (false, false, false) => "r",
        };
        let mut row = PgClassRow::new(
            crate::catalog_rel::table_relation_oid(table.id)?,
            &table.name.name,
            relkind,
            crate::catalog_rel::namespace_oid(&table.name.schema),
        );
        // A materialized view is rewritten by a rule the way an ordinary view
        // is, so PostgreSQL reports `relhasrules` for one; and its contents
        // exist only once `REFRESH` has run, which is the whole meaning of
        // `relispopulated` — every other relation kind is populated by
        // definition.
        if let Some(matview) = &table.materialized {
            row.relhasrules = true;
            row.relispopulated = matview.populated;
        }
        row.relnatts = table.columns.len();
        row.relchecks = table.checks.len();
        row.relhasindex = indexed_table_ids.contains(&table.id);
        row.relhastriggers = triggered_relation_ids.contains(&table.id);
        // The same read answers both: a relation is a partition exactly when it
        // has a stored bound, and that bound is what `relpartbound` reports.
        if let Some((_, bound)) = crate::partition::parent_of(catalog_kv, &table.name)? {
            row.relispartition = true;
            row.relpartbound = Some(crate::partition::bound_text(&bound));
        }
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&table.name.schema);
        row.reltablespace = crabka_pgcatalog::relation_tablespace_oid(catalog_kv, &table.name)?;
        row.relowner = role_oid_of(&role_oids, &table.owner);
        row.relrowsecurity = table.row_security;
        row.relforcerowsecurity = table.force_row_security;
        let stats = relstats.get(&table.name).copied().unwrap_or_default();
        row.reltuples = stats.reltuples;
        row.relpages = stats.relpages;
        row.relallvisible = stats.relallvisible;
        row.relhassubclass = stats.has_subclass;
        // A partitioned table holds no rows of its own, and a foreign table
        // holds none here at all, so neither carries a store. Every other
        // stored kind — an ordinary relation, a partition, a materialized
        // view — is measured.
        if matches!(relkind, "r" | "m") && needs_toast_relation(&table) {
            row.reltoastrelid = toast_relation_oid(table.id)?;
        }
        table_owner_oids.insert(table.name.clone(), row.relowner);
        rows.push(row.build()?);
    }
    for view in crabka_pgcatalog::list_views(catalog_kv)? {
        let oid = crate::catalog_rel::view_oids(catalog_kv)?
            .get(&view.name)
            .copied()
            .unwrap_or(0);
        let mut row = PgClassRow::new(
            oid,
            &view.name.name,
            "v",
            crate::catalog_rel::namespace_oid(&view.name.schema),
        );
        row.relnatts = view.columns.len();
        row.relowner = role_oid_of(&role_oids, &view.owner);
        row.relhasrules = true;
        row.relhastriggers =
            u32::try_from(oid).is_ok_and(|oid| triggered_relation_ids.contains(&oid));
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&view.name.schema);
        rows.push(row.build()?);
    }
    for (name, _) in crabka_pgcatalog::list_sequences(catalog_kv)? {
        let oid = crate::catalog_rel::sequence_oids(catalog_kv)?
            .get(&name)
            .copied()
            .unwrap_or(0);
        let mut row = PgClassRow::new(
            oid,
            &name.name,
            "S",
            crate::catalog_rel::namespace_oid(&name.schema),
        );
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&name.schema);
        rows.push(row.build()?);
    }
    for ty in crabka_pgcatalog::list_user_types(catalog_kv)? {
        let Some(fields) = ty.fields() else { continue };
        let type_oid = i32::try_from(ty.oid)
            .map_err(|_| ExecError::Unsupported("composite type oid exceeds int4".into()))?;
        let oid = i32::try_from(crabka_pgtypes::usertype::composite_relation_oid(ty.oid))
            .map_err(|_| ExecError::Unsupported("composite relation oid exceeds int4".into()))?;
        let mut row = PgClassRow::new(
            oid,
            &ty.name,
            "c",
            crate::catalog_rel::namespace_oid(&ty.schema),
        );
        row.reltype = type_oid;
        row.relnatts = fields.len();
        rows.push(row.build()?);
    }
    for virtual_table in virtual_table_names() {
        let table = virtual_catalog_table(virtual_table);
        let oid = virtual_relation_oid(virtual_table);
        let (relkind, relfilenode) = virtual_pg_class_properties(virtual_table, oid);
        let mut row = PgClassRow::new(
            oid,
            &table.name.name,
            relkind,
            virtual_relation_namespace_oid(virtual_table),
        );
        row.relnatts = table.columns.len();
        row.relfilenode = relfilenode;
        row.relhasindex = builtin_catalog_oid_index(virtual_table).is_some();
        rows.push(row.build()?);
    }
    for index in BUILTIN_CATALOG_OID_INDEXES {
        let mut row = PgClassRow::new(index.oid, index.name, "i", PG_CATALOG_NAMESPACE_OID);
        row.relnatts = 1;
        row.relam = crate::catalog_rel::BTREE_AM_OID;
        if virtual_pg_class_properties(index.table, virtual_relation_oid(index.table)).1 == 0 {
            row.relfilenode = 0;
        }
        rows.push(row.build()?);
    }
    for index in indexes {
        // An index lives in the schema of the table it indexes, which is also
        // what makes a temporary table's index temporary.
        let relkind = if crate::partition::is_partitioned(catalog_kv, &index.table)? {
            "I"
        } else {
            "i"
        };
        let mut row = PgClassRow::new(
            catalog_index_oid(index.id)?,
            &index.name,
            relkind,
            crate::catalog_rel::namespace_oid(&index.table.schema),
        );
        row.relnatts = index.columns.len();
        row.relam = match index.method {
            crabka_pgcatalog::IndexMethod::Btree => crate::catalog_rel::BTREE_AM_OID,
            crabka_pgcatalog::IndexMethod::Hash => crate::catalog_rel::HASH_AM_OID,
            crabka_pgcatalog::IndexMethod::Gist => crate::catalog_rel::GIST_AM_OID,
            crabka_pgcatalog::IndexMethod::Gin => crate::catalog_rel::GIN_AM_OID,
            crabka_pgcatalog::IndexMethod::Spgist => crate::catalog_rel::SPGIST_AM_OID,
        };
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&index.table.schema);
        row.reltablespace =
            crabka_pgcatalog::relation_tablespace_oid(catalog_kv, &index.qualified_name())?;
        row.relowner = table_owner_oids
            .get(&index.table)
            .copied()
            .unwrap_or(crate::catalog_fn::BOOTSTRAP_ROLE_OID);
        rows.push(row.build()?);
    }
    Ok(rows)
}

/// The `pg_authid.oid` an owning role projects as. A name no role row answers
/// to — the role was dropped out from under the relation — falls back to the
/// bootstrap superuser, which is the same fallback `pg_tablespace` takes.
pub(crate) fn role_oid_of(role_oids: &std::collections::BTreeMap<String, i32>, owner: &str) -> i32 {
    role_oids
        .get(owner)
        .copied()
        .unwrap_or(crate::catalog_fn::BOOTSTRAP_ROLE_OID)
}

/// PostgreSQL catalogs are base relations except for its SQL views. Relation-
/// mapped catalogs retain `relfilenode = 0`; ordinary catalogs use their oid.
pub(crate) fn virtual_pg_class_properties(name: &str, oid: i32) -> (&'static str, i32) {
    let is_view = name.starts_with("information_schema.")
        || matches!(
            name,
            "pg_indexes"
                | "pg_locks"
                | "pg_matviews"
                | "pg_policies"
                | "pg_prepared_statements"
                | "pg_replication_slots"
                | "pg_roles"
                | "pg_settings"
                | "pg_shmem_allocations_numa"
                | "pg_stat_activity"
                | "pg_tables"
                | "pg_user"
                | "pg_views"
        );
    let is_mapped = matches!(
        name,
        "pg_attribute"
            | "pg_authid"
            | "pg_class"
            | "pg_database"
            | "pg_proc"
            | "pg_shdescription"
            | "pg_tablespace"
            | "pg_type"
    );
    if is_view {
        ("v", 0)
    } else {
        ("r", if is_mapped { 0 } else { oid })
    }
}

/// First `pg_class` oid of the band reserved for TOAST relations.
///
/// The band sits above every other one [`crate::catalog_rel`] hands out, and is
/// the same 9,000 wide, so a table's TOAST oid is its catalog id offset by this
/// base — stable across restarts, and distinct per relation.
///
/// No `pg_class` row carries one of these oids. crabka stores wide values
/// inline, so the TOAST relation the oid names does not exist; the oid records
/// only that `PostgreSQL` would have built one. A query that joins
/// `reltoastrelid` back to `pg_class.oid` therefore finds nothing — which is
/// the same nothing the zero it replaces found.
const TOAST_OID_BASE: i32 = 160_000;

/// Width of the TOAST oid band, which mirrors `catalog_rel`'s.
const TOAST_OID_BAND_WIDTH: u32 = 9_000;

/// `PostgreSQL`'s `TOAST_TUPLE_THRESHOLD`: `MaximumBytesPerTuple(4)` over the
/// default 8 kB block, which is the width above which a heap tuple is worth an
/// out-of-line store.
const TOAST_TUPLE_THRESHOLD: i32 = 2032;

/// `pg_encoding_max_length(UTF8)`. `server_encoding` is a fixed `UTF8` here, so
/// the widest byte length of one character is a constant rather than a lookup.
const MAX_BYTES_PER_CHARACTER: i32 = 4;

/// The `pg_class` oid of a relation's TOAST relation: its catalog id inside the
/// TOAST band.
///
/// # Errors
///
/// Returns `0A000` when the catalog id is wider than the band, exactly as the
/// relation's own oid does.
pub(crate) fn toast_relation_oid(table_id: u32) -> Result<i32, ExecError> {
    i32::try_from(table_id)
        .ok()
        .filter(|_| table_id < TOAST_OID_BAND_WIDTH)
        .and_then(|id| TOAST_OID_BASE.checked_add(id))
        .ok_or_else(|| ExecError::Unsupported("toast oid leaves its band".into()))
}

/// Whether a relation of this shape gets a TOAST relation, which is
/// `heapam_relation_needs_toast_table` read off the column list.
///
/// Three questions in order, and the order is the whole rule. A relation with
/// no column that can go out of line never gets one. One that has a column of
/// unbounded width always does. Otherwise the widest tuple the bounded columns
/// can build decides it, against a quarter of a block.
///
/// The widths are `pg_type.typlen` and `pg_attribute.atttypmod`, as
/// PostgreSQL's are. The *alignment* is not: crabka publishes `attalign = 'i'`
/// for every column, so this counts four-byte padding throughout rather than
/// each type's own. Only a relation whose bounded columns land within a few
/// bytes of the threshold can tell the two apart.
pub(crate) fn needs_toast_relation(table: &Table) -> bool {
    let mut data_length: i32 = 0;
    let mut unbounded = false;
    let mut toastable = false;
    for column in &table.columns {
        // A virtual generated column occupies no tuple space, so it is not
        // measured and cannot be what makes the relation need a store.
        if column.attgenerated() == "v" {
            continue;
        }
        data_length = align_to(data_length, 4);
        let typlen = i32::from(column.ty.type_size());
        if typlen > 0 {
            data_length = data_length.saturating_add(typlen);
            continue;
        }
        match type_maximum_size(column.ty) {
            Some(max) => data_length = data_length.saturating_add(max),
            None => unbounded = true,
        }
        toastable |= attribute_storage(column.ty) != "p";
    }
    if !toastable {
        return false;
    }
    if unbounded {
        return true;
    }
    let natts = i32::try_from(table.columns.len()).unwrap_or(i32::MAX);
    // `MAXALIGN(SizeofHeapTupleHeader + BITMAPLEN(natts)) + MAXALIGN(data_length)`.
    // The null bitmap covers every attribute, virtual ones included.
    let header = align_to(23i32.saturating_add(natts.saturating_add(7) / 8), 8);
    header.saturating_add(align_to(data_length, 8)) > TOAST_TUPLE_THRESHOLD
}

/// Round `value` up to the next multiple of `alignment`, which must be a power
/// of two. Saturates rather than wrapping, so a width near `i32::MAX` stays
/// above the threshold instead of folding below it.
pub(crate) fn align_to(value: i32, alignment: i32) -> i32 {
    value
        .saturating_add(alignment - 1)
        .saturating_sub((value.saturating_add(alignment - 1)) % alignment)
}

/// `PostgreSQL`'s `type_maximum_size`: the widest a value of this type can be,
/// or `None` for a type whose width has no bound.
///
/// Only four types answer at all, and each in its own unit. `character(n)` and
/// `character varying(n)` count *characters*, so the bound is `n` times the
/// widest character the encoding has, plus the varlena header. `bit(n)` and
/// `bit varying(n)` count bits. `numeric(p, s)` is the worst-case digit array.
/// Every other variable-width type — `text`, `bytea`, `json`, an array — has no
/// bound at all, which is what makes a relation carrying one need a TOAST
/// relation outright.
pub(crate) fn type_maximum_size(ty: ColumnType) -> Option<i32> {
    /// `VARHDRSZ`.
    const VARHDRSZ: i32 = 4;

    match ty {
        ColumnType::Char(Some(n)) | ColumnType::Varchar(Some(n)) => {
            Some(i32::from(n) * MAX_BYTES_PER_CHARACTER + VARHDRSZ)
        }
        ColumnType::Bit(Some(n)) | ColumnType::VarBit(Some(n)) => {
            Some(n.saturating_add(7) / 8 + 2 * 4)
        }
        // `numeric_maximum_size`: four decimal digits per 16-bit `NumericDigit`,
        // one spare digit for an unaligned decimal point and one more for the
        // rounding, over an 8-byte `NUMERIC_HDRSZ`.
        ColumnType::Numeric(Some(typmod)) => {
            let digits = (i32::from(typmod.precision) + 4 + 3) / 4;
            Some(8 + digits * 2)
        }
        ColumnType::Domain(domain) => type_maximum_size(*domain.base),
        _ => None,
    }
}

/// The handful of `pg_class` fields that actually vary between crabka's
/// relation kinds. Everything else in the row is the same constant for all of
/// them, and [`PgClassRow::build`] writes it.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct PgClassRow<'a> {
    pub(crate) oid: i32,
    pub(crate) relname: &'a str,
    pub(crate) relkind: &'a str,
    pub(crate) relnamespace: i32,
    pub(crate) reltype: i32,
    pub(crate) relnatts: usize,
    pub(crate) relchecks: usize,
    pub(crate) relhasindex: bool,
    pub(crate) relhasrules: bool,
    pub(crate) relhastriggers: bool,
    /// `pg_class.relhassubclass`. A latch set when a child appears and cleared
    /// only by an `ANALYZE` that finds none left — see [`crate::relstats`].
    /// Only a stored relation carries one; a partitioned *index* also does in
    /// `PostgreSQL`, and that is not modelled here.
    pub(crate) relhassubclass: bool,
    /// `pg_class.reltuples`. [`crate::relstats::UNKNOWN_TUPLES`] until an
    /// `ANALYZE` measures the relation.
    pub(crate) reltuples: f32,
    /// `pg_class.relpages`, stored with `reltuples`.
    pub(crate) relpages: i32,
    /// `pg_class.relallvisible`, stored with `reltuples`.
    pub(crate) relallvisible: i32,
    pub(crate) relam: i32,
    pub(crate) relfilenode: i32,
    pub(crate) relispartition: bool,
    /// `pg_class.relrowsecurity` / `relforcerowsecurity`. Only stored relations
    /// can carry row security; every other relation kind leaves these false.
    pub(crate) relrowsecurity: bool,
    pub(crate) relforcerowsecurity: bool,
    /// `pg_class.relispopulated`. Only a materialized view can be unpopulated —
    /// every other relation kind holds whatever it holds the moment it exists —
    /// so this defaults true and only `relkind = 'm'` ever clears it.
    pub(crate) relispopulated: bool,
    pub(crate) reltablespace: u32,
    /// The `pg_authid.oid` of the owning role. Only stored relations carry a
    /// real owner; the catalog's own relations belong to the bootstrap
    /// superuser, which is the default here.
    pub(crate) relowner: i32,
    /// `p` for an ordinary relation, `t` for one in a session's temporary
    /// namespace. That is where every temporary relation is, so the schema is
    /// the whole fact and nothing stores it twice.
    pub(crate) relpersistence: char,
    /// `pg_class.relpartbound`, already deparsed. PostgreSQL stores a node tree
    /// and hands it to `pg_get_expr`; crabka's `pg_get_expr` is the identity,
    /// so the column carries the printed clause. Only a partition has one.
    pub(crate) relpartbound: Option<String>,
    /// `pg_class.reltoastrelid`, from [`toast_relation_oid`]. Zero for every
    /// relation kind that cannot hold one — a partitioned table, a view, a
    /// sequence — and for a stored relation whose columns all fit inline.
    pub(crate) reltoastrelid: i32,
}

impl<'a> PgClassRow<'a> {
    pub(crate) fn new(oid: i32, relname: &'a str, relkind: &'a str, relnamespace: i32) -> Self {
        let relfilenode = match relkind {
            "v" | "c" | "f" | "p" | "I" => 0,
            _ => oid,
        };
        Self {
            oid,
            relname,
            relkind,
            relnamespace,
            reltype: 0,
            relnatts: 0,
            relchecks: 0,
            relhasindex: false,
            relhasrules: false,
            relhastriggers: false,
            relhassubclass: false,
            reltuples: crate::relstats::UNKNOWN_TUPLES,
            relpages: 0,
            relallvisible: 0,
            // A materialized view's contents live in the heap exactly as a
            // table's do, so PostgreSQL gives it the heap access method too.
            relam: if matches!(relkind, "r" | "m") { 2 } else { 0 },
            relfilenode,
            relispartition: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            reltablespace: 0,
            relowner: crate::catalog_fn::BOOTSTRAP_ROLE_OID,
            relpersistence: 'p',
            relpartbound: None,
            reltoastrelid: 0,
        }
    }

    pub(crate) fn build(self) -> Result<Vec<Datum>, ExecError> {
        let natts = i16::try_from(self.relnatts)
            .map_err(|_| ExecError::Unsupported("relnatts exceeds int2 range".into()))?;
        let checks = i16::try_from(self.relchecks)
            .map_err(|_| ExecError::Unsupported("relchecks exceeds int2 range".into()))?;
        Ok(vec![
            int(self.oid),
            text(self.relname),
            int(self.relnamespace),
            int(self.reltype),
            int(0),
            int(self.relowner),
            int(self.relam),
            int(self.relfilenode),
            int(i32::try_from(self.reltablespace)
                .map_err(|_| ExecError::Unsupported("tablespace oid exceeds int4".into()))?),
            int(self.relpages),
            Datum::Float4(self.reltuples),
            int(self.relallvisible),
            int(0),
            int(self.reltoastrelid),
            Datum::Bool(self.relhasindex),
            Datum::Bool(false),
            // Every crabka relation is replica-identity "default"; its
            // persistence follows the schema holding it.
            text(&self.relpersistence.to_string()),
            text(self.relkind),
            Datum::Int2(natts),
            Datum::Int2(checks),
            Datum::Bool(self.relhasrules),
            Datum::Bool(self.relhastriggers),
            Datum::Bool(self.relhassubclass),
            Datum::Bool(self.relrowsecurity),
            Datum::Bool(self.relforcerowsecurity),
            Datum::Bool(self.relispopulated),
            text("d"),
            Datum::Bool(self.relispartition),
            int(0),
            Datum::Int8(0),
            Datum::Int8(0),
            // relacl, reloptions.
            Datum::Null,
            Datum::Null,
            self.relpartbound.as_deref().map_or(Datum::Null, text),
        ])
    }
}

pub(crate) fn pg_attribute_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    // Read once for the whole projection rather than per relation: the ACL is
    // one flat namespace and a per-relation scan would reread it for every
    // table in the database.
    let acl = ColumnAcl::read(catalog_kv)?;
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        rows.extend(attribute_rows_for_table(
            crate::catalog_rel::table_relation_oid(table.id)?,
            &table,
            &acl,
        )?);
    }
    // A view's columns are `pg_attribute` rows like any other relation's —
    // that is where `\d+`, `information_schema.columns` and every driver's
    // introspection read them from. They carry no default and no NOT NULL,
    // which is what a `View`'s own column list already says.
    let view_oids = crate::catalog_rel::view_oids(catalog_kv)?;
    for view in crabka_pgcatalog::list_views(catalog_kv)? {
        let Some(oid) = view_oids.get(&view.name).copied() else {
            continue;
        };
        let table = crabka_pgcatalog::Table {
            id: 0,
            owner: view.owner.clone(),
            name: view.name.clone(),
            columns: view.columns,
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        };
        rows.extend(attribute_rows_for_table(oid, &table, &acl)?);
    }
    for virtual_table in virtual_table_names() {
        let table = virtual_catalog_table(virtual_table);
        rows.extend(attribute_rows_for_table(
            virtual_relation_oid(virtual_table),
            &table,
            &acl,
        )?);
    }
    for index in BUILTIN_CATALOG_OID_INDEXES {
        let table = builtin_catalog_index_table(index);
        rows.extend(attribute_rows_for_table(index.oid, &table, &acl)?);
    }
    // A composite type's attributes hang off the relation its `pg_type.typrelid`
    // points at, which is how `\d <type>` and the driver introspection queries
    // reach them.
    for ty in crabka_pgcatalog::list_user_types(catalog_kv)? {
        let Some(fields) = ty.fields() else { continue };
        let relid = i32::try_from(crabka_pgtypes::usertype::composite_relation_oid(ty.oid))
            .map_err(|_| ExecError::Unsupported("composite relation oid exceeds int4".into()))?;
        let table = crabka_pgcatalog::Table {
            id: 0,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: crabka_pgcatalog::RelationName::new(ty.schema.clone(), ty.name.clone()),
            columns: fields
                .iter()
                .map(|field| crabka_pgcatalog::Column::new(field.name.clone(), field.ty))
                .collect(),
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        };
        rows.extend(attribute_rows_for_table(relid, &table, &acl)?);
    }
    Ok(rows)
}

/// The standard's view of the same schemas `pg_namespace` lists, so a schema
/// created here appears and a dropped `public` disappears. PostgreSQL builds
/// this view by joining `pg_namespace.nspowner` to `pg_authid`, so
/// `schema_owner` is exactly what `pg_get_userbyid(nspowner)` answers. The
/// character-set columns and `sql_path` are NULL there too.
pub(crate) fn information_schema_schemata_rows(
    catalog_kv: &dyn Kv,
    database: &str,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_schemas(catalog_kv)?
        .into_iter()
        .map(|schema| {
            vec![
                text(database),
                text(&schema.name),
                text(schema_owner_name(&schema.owner)),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]
        })
        .collect())
}

/// The role name behind [`schema_owner_oid`], so the two schema projections
/// cannot disagree about who owns a schema.
pub(crate) fn schema_owner_name(owner: &str) -> &'static str {
    if owner == crabka_pgcatalog::PUBLIC_SCHEMA_OWNER {
        crabka_pgcatalog::PUBLIC_SCHEMA_OWNER
    } else {
        crate::catalog_fn::OBJECT_OWNER
    }
}

/// True when `schema` is a temporary namespace belonging to some *other*
/// session. This is `PostgreSQL`'s `pg_is_other_temp_schema`, which its
/// `information_schema` views filter relations on.
///
/// `pg_class`, `pg_namespace` and `information_schema.schemata` do not filter:
/// on `postgres:18.4` another session's temporary relation is visible in
/// `pg_class` and its namespace in all three. Only the standard's relation
/// views hide it.
pub(crate) fn is_other_temp_schema(schema: &str, backend_id: i32) -> bool {
    crabka_pgcatalog::is_temp_schema(schema)
        && schema != crabka_pgcatalog::temp_schema_name(backend_id)
}

/// Every relation the SQL standard calls a table: base tables, foreign tables,
/// and — F-2 — views, which `table_type = 'VIEW'` distinguishes.
///
/// A materialized view is not one of them. The standard has no such object, and
/// PostgreSQL's `information_schema.tables` filters `relkind` to `r`/`p`/`v`/`f`
/// rather than inventing a `table_type` for it, so one is absent here as well as
/// from `information_schema.columns`.
pub(crate) fn information_schema_tables_rows(
    catalog_kv: &dyn Kv,
    database: &str,
    backend_id: i32,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = crabka_pgcatalog::list_tables(catalog_kv)?
        .into_iter()
        .filter(|table| {
            !is_other_temp_schema(&table.name.schema, backend_id) && table.materialized.is_none()
        })
        .map(|table| {
            information_schema_table_row(
                database,
                &table.name,
                if table.foreign.is_some() {
                    "FOREIGN"
                } else {
                    "BASE TABLE"
                },
                // Every ordinary table takes an INSERT; a foreign one takes
                // whichever the scanner admits, and this engine's scanners
                // admit none, so the auto-updatable test answers for both.
                table.foreign.is_none(),
            )
        })
        .collect::<Vec<_>>();
    rows.extend(
        crabka_pgcatalog::list_views(catalog_kv)?
            .into_iter()
            .filter(|view| !is_other_temp_schema(&view.name.schema, backend_id))
            .map(|view| {
                let insertable = crate::viewwrite::relation_updatable_events(
                    catalog_kv, &view.name, false, None, 0,
                ) & crate::viewwrite::INSERT_EVENT
                    != 0;
                information_schema_table_row(database, &view.name, "VIEW", insertable)
            }),
    );
    Ok(rows)
}

pub(crate) fn information_schema_table_row(
    database: &str,
    name: &crabka_pgcatalog::RelationName,
    table_type: &str,
    insertable: bool,
) -> Vec<Datum> {
    vec![
        text(database),
        text(&name.schema),
        text(&name.name),
        text(table_type),
        text(yes_no(insertable)),
    ]
}

/// The standard's `yes_or_no` domain, which every `information_schema` boolean
/// is spelled in.
const fn yes_no(flag: bool) -> &'static str {
    if flag { "YES" } else { "NO" }
}

pub(crate) fn information_schema_columns_rows(
    catalog_kv: &dyn Kv,
    backend_id: i32,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        // A materialized view contributes no rows here for the same reason it
        // contributes none to `information_schema.tables`: the standard has no
        // such relation, so PostgreSQL leaves it out of both.
        if is_other_temp_schema(&table.name.schema, backend_id) || table.materialized.is_some() {
            continue;
        }
        for (idx, column) in table.columns.iter().enumerate() {
            rows.push(information_schema_column_row(
                catalog_kv,
                &table.name,
                column,
                idx,
                // Every column of an ordinary table is updatable, so the
                // per-column predicate is not consulted for one.
                true,
            )?);
        }
    }
    // A view's columns belong here too — `is_updatable` is a per-column answer
    // and a view is where it stops being uniformly YES.
    for view in crabka_pgcatalog::list_views(catalog_kv)? {
        if is_other_temp_schema(&view.name.schema, backend_id) {
            continue;
        }
        for (idx, column) in view.columns.iter().enumerate() {
            let updatable = crate::viewwrite::column_is_updatable(
                catalog_kv,
                &view.name,
                usize_i32(idx + 1)?,
                false,
            );
            rows.push(information_schema_column_row(
                catalog_kv, &view.name, column, idx, updatable,
            )?);
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_column_row(
    catalog_kv: &dyn Kv,
    relation: &crabka_pgcatalog::RelationName,
    column: &crabka_pgcatalog::Column,
    index: usize,
    updatable: bool,
) -> Result<Vec<Datum>, ExecError> {
    Ok(vec![
        text(&relation.schema),
        text(&relation.name),
        text(&column.name),
        int(usize_i32(index + 1)?),
        // PostgreSQL reports the literal string `ARRAY` here for every array
        // column (the element type lives in `udt_name`, which this synthesized
        // view does not expose).
        text(match column.ty {
            ColumnType::Array(_) => "ARRAY",
            ty => ty.name(),
        }),
        text(if column.not_null { "NO" } else { "YES" }),
        column_default_datum(catalog_kv, column),
        text(yes_no(updatable)),
    ])
}

pub(crate) fn information_schema_trigger_rows(
    catalog_kv: &dyn Kv,
    database: &str,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgcatalog::trigger::{TriggerLevel, TriggerTiming};
    let triggers = crabka_pgcatalog::trigger::list_triggers(catalog_kv)?;
    let mut rows = Vec::new();
    for trigger in triggers.iter().filter(|trigger| !trigger.is_internal) {
        let function = crate::routine::routine_by_oid(
            catalog_kv,
            i32::try_from(trigger.function_oid).unwrap_or(0),
        )?
        .map_or_else(|| trigger.function.clone(), |routine| routine.name);
        let events = [
            (trigger.events.insert, "INSERT"),
            (trigger.events.update, "UPDATE"),
            (trigger.events.delete, "DELETE"),
            (trigger.events.truncate, "TRUNCATE"),
        ];
        for (_, event) in events.into_iter().filter(|(enabled, _)| *enabled) {
            let action_order = triggers
                .iter()
                .filter(|candidate| {
                    candidate.table_id == trigger.table_id
                        && candidate.timing == trigger.timing
                        && candidate.level == trigger.level
                        && candidate.name <= trigger.name
                        && match event {
                            "INSERT" => candidate.events.insert,
                            "UPDATE" => candidate.events.update,
                            "DELETE" => candidate.events.delete,
                            _ => candidate.events.truncate,
                        }
                })
                .count();
            let arguments = trigger
                .arguments
                .iter()
                .map(|argument| format!("'{}'", argument.replace(char::from(39), "''")))
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(vec![
                text(database),
                text(&trigger.table.schema),
                text(&trigger.name),
                text(event),
                text(database),
                text(&trigger.table.schema),
                text(&trigger.table.name),
                Datum::Int4(i32::try_from(action_order).unwrap_or(i32::MAX)),
                trigger
                    .when
                    .as_ref()
                    .map_or(Datum::Null, |value| text(value)),
                text(&format!("EXECUTE FUNCTION {function}({arguments})")),
                text(match trigger.level {
                    TriggerLevel::Row => "ROW",
                    TriggerLevel::Statement => "STATEMENT",
                }),
                text(match trigger.timing {
                    TriggerTiming::Before => "BEFORE",
                    TriggerTiming::After => "AFTER",
                    TriggerTiming::InsteadOf => "INSTEAD OF",
                }),
                trigger
                    .old_transition
                    .as_ref()
                    .map_or(Datum::Null, |value| text(value)),
                trigger
                    .new_transition
                    .as_ref()
                    .map_or(Datum::Null, |value| text(value)),
                if trigger.level == TriggerLevel::Row {
                    text("OLD")
                } else {
                    Datum::Null
                },
                if trigger.level == TriggerLevel::Row {
                    text("NEW")
                } else {
                    Datum::Null
                },
                Datum::Null,
            ]);
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_triggered_update_column_rows(
    catalog_kv: &dyn Kv,
    database: &str,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for trigger in crabka_pgcatalog::trigger::list_triggers(catalog_kv)?
        .into_iter()
        .filter(|trigger| !trigger.is_internal && trigger.events.update)
    {
        for column in &trigger.events.update_columns {
            rows.push(vec![
                text(database),
                text(&trigger.table.schema),
                text(&trigger.name),
                text(database),
                text(&trigger.table.schema),
                text(&trigger.table.name),
                text(column),
            ]);
        }
    }
    Ok(rows)
}

pub(crate) fn column_default_datum(catalog_kv: &dyn Kv, column: &Column) -> Datum {
    let Some(default) = &column.default else {
        return Datum::Null;
    };
    text(&format_column_default(catalog_kv, default, column.ty))
}

pub(crate) fn format_column_default(
    catalog_kv: &dyn Kv,
    default: &ColumnDefault,
    ty: ColumnType,
) -> String {
    match default {
        ColumnDefault::NextVal(sequence) => {
            format!("nextval('{}'::regclass)", escape_sql_string(sequence))
        }
        // Only the oid is stored, so the name is read from the catalog now —
        // the same output-time resolution `pg_get_expr` performs.
        //
        // The default scope, not the reader's: a deparsed default is a property
        // of the column and every session that reads `information_schema` has to
        // be told the same text, where `regclassout` deliberately answers each
        // session in its own search path. This is the rule the whole of
        // `regclassout` used before it became visibility-aware.
        ColumnDefault::Value(Datum::Regclass(value)) => {
            let resolved = regclass_by_oid(
                catalog_kv,
                crate::relname::ResolutionScope::default_scope(),
                value.oid,
            )
            .unwrap_or_else(|_| crabka_pgtypes::RegclassValue::unresolved(value.oid));
            format!("'{}'::{}", escape_sql_string(&resolved.name), ty.name())
        }
        ColumnDefault::Value(value) => format_default_value(value, ty),
    }
}

/// A type name as `pg_get_expr` renders it in a default expression: quoted
/// when the word is reserved, which for the types crabka has is `bit` alone —
/// `'1001'::"bit"`, but `'1001'::bit varying`.
pub(crate) fn quoted_type_name(ty: ColumnType) -> String {
    match ty {
        ColumnType::Bit(_) => "\"bit\"".to_string(),
        other => other.name().to_string(),
    }
}

pub(crate) fn format_default_value(value: &Datum, ty: ColumnType) -> String {
    match value {
        Datum::Null => "NULL".to_string(),
        Datum::Bool(true) => "true".to_string(),
        Datum::Bool(false) => "false".to_string(),
        Datum::Int2(value) => value.to_string(),
        Datum::Int4(value) => value.to_string(),
        Datum::Int8(value) => value.to_string(),
        // Both float widths render through their own output function so a
        // `real` default reads back as PostgreSQL spells it (`1e+06`, not
        // `1000000`).
        Datum::Float4(_) | Datum::Float8(_) => String::from_utf8(
            crabka_pgtypes::encoding::encode_text(value, &jiff::tz::TimeZone::UTC),
        )
        .expect("a Datum's text encoding is always valid UTF-8"),
        Datum::Numeric(value) => value.to_string(),
        Datum::Text(value) | Datum::JsonPath(value) => {
            let mut out = String::new();
            let _ = write!(out, "'{}'::{}", escape_sql_string(value), ty.name());
            out
        }
        // A json/jsonb/xml/array default renders like PostgreSQL's
        // `pg_get_expr` output: the value's own text, quoted and cast to the
        // column type.
        Datum::Json(_)
        | Datum::Xml(_)
        | Datum::Jsonb(_)
        // `polygon` is a varlena whose output function is zone-independent, so
        // its default reads back the way `pg_get_expr` prints it:
        // `'((0,0),(1,1))'::polygon`.
        | Datum::Polygon(_)
        | Datum::Array(_)
        | Datum::OidVector(_)
        | Datum::Range(_)
        | Datum::Multirange(_)
        | Datum::TsVector(_)
        | Datum::TsQuery(_)
        | Datum::Inet(_)
        | Datum::MacAddr(_)
        | Datum::MacAddr8(_)
        | Datum::BitString(_)
        | Datum::Money(_)
        // `pg_get_expr` prints a `"char"` default as `'r'::"char"` — the
        // escaped text form, quoted and cast, exactly like the rest of this
        // group. `quoted_type_name` needs no arm of its own for it: the type's
        // name already carries the double quotes.
        | Datum::InternalChar(_)
        | Datum::Oid(_)
        | Datum::Xid(_)
        | Datum::Xid8(_)
        | Datum::Cid(_)
        | Datum::Tid(_)
        | Datum::PgLsn(_)
        // A snapshot's text form is its canonical form, so a default reads
        // back exactly as it was written: `'12:20:13,15,18'::pg_snapshot`.
        | Datum::PgSnapshot(_) => {
            match zone_independent_text(value) {
                Some(literal) => {
                    let mut out = String::new();
                    let _ = write!(
                        out,
                        "'{}'::{}",
                        escape_sql_string(&literal),
                        quoted_type_name(ty)
                    );
                    out
                }
                None => "<unsupported>".to_string(),
            }
        }
        Datum::Date(_)
        | Datum::Point(_)
        | Datum::Path(_)
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
        // A `regclass` default is rendered by `format_column_default`, which has
        // the catalog handle its name needs.
        | Datum::Regclass(_)
        | Datum::Bytea(_) => "<unsupported>".to_string(),
    }
}

/// The output text of a value whose rendering does not depend on the session
/// time zone, for the catalog's default-expression rendering (which has no
/// session context). `None` for a `timestamptz` array element, the one case a
/// jsonb/array value can be zone-dependent.
pub(crate) fn zone_independent_text(value: &Datum) -> Option<String> {
    fn zone_dependent(value: &Datum) -> bool {
        match value {
            Datum::Timestamptz(_) => true,
            Datum::Array(array) => array.elems.iter().any(zone_dependent),
            Datum::Range(range) => range
                .lower
                .iter()
                .chain(&range.upper)
                .any(|bound| zone_dependent(bound)),
            _ => false,
        }
    }
    if zone_dependent(value) {
        return None;
    }
    String::from_utf8(crabka_pgtypes::encoding::encode_text(
        value,
        &jiff::tz::TimeZone::UTC,
    ))
    .ok()
}

pub(crate) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// PostgreSQL 18.4's `pg_attribute` row per column. `attidentity` and
/// `attgenerated` carry the empty string for an ordinary column, which is what
/// PostgreSQL stores and what `\d`'s "Generated"/"Identity" columns test.
/// Every column-level grant in the database, keyed the way `pg_attribute`
/// needs to read it back: relation, then column.
///
/// This is `pg_attribute.attacl`. It stays NULL for a column nobody has
/// granted, which is what `PostgreSQL` stores and what `pg_dump` tests before
/// it emits a `GRANT`.
pub(crate) struct ColumnAcl(
    std::collections::BTreeMap<(crabka_pgcatalog::RelationName, String), Vec<String>>,
);

impl ColumnAcl {
    pub(crate) fn read(catalog_kv: &dyn Kv) -> Result<Self, ExecError> {
        let mut grouped: std::collections::BTreeMap<_, Vec<String>> =
            std::collections::BTreeMap::new();
        for privilege in crabka_pgcatalog::list_column_privileges(catalog_kv)? {
            grouped
                .entry((privilege.table, privilege.column))
                .or_default()
                .push(format!(
                    "{}={}/{}",
                    privilege.grantee,
                    acl_privilege_letter(&privilege.privilege),
                    crabka_pgcatalog::BOOTSTRAP_ROLE,
                ));
        }
        for entry in grouped.values_mut() {
            entry.sort();
        }
        Ok(Self(grouped))
    }

    pub(crate) fn of(&self, table: &crabka_pgcatalog::RelationName, column: &str) -> Datum {
        self.0
            .get(&(table.clone(), column.to_string()))
            .map_or(Datum::Null, |items| {
                Datum::Array(crabka_pgtypes::ArrayValue::new(
                    crabka_pgtypes::ElemType::Text,
                    items.iter().map(|item| Datum::Text(item.clone())).collect(),
                ))
            })
    }
}

/// The `aclitem` letter `PostgreSQL` prints for a privilege. Only the four it
/// allows on a column can reach here, and the catalog refuses to store any
/// other, so the fallback is unreachable rather than a silent mis-spelling.
pub(crate) fn acl_privilege_letter(privilege: &str) -> &'static str {
    match privilege.to_ascii_uppercase().as_str() {
        "SELECT" => "r",
        "INSERT" => "a",
        "UPDATE" => "w",
        "REFERENCES" => "x",
        _ => "?",
    }
}

pub(crate) fn attribute_rows_for_table(
    relid: i32,
    table: &Table,
    acl: &ColumnAcl,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let attnum = i16::try_from(idx + 1)
                .map_err(|_| ExecError::Unsupported("attnum exceeds int2 range".into()))?;
            let identity = match column.identity {
                Some(crabka_pgcatalog::IdentityKind::Always) => "a",
                Some(crabka_pgcatalog::IdentityKind::ByDefault) => "d",
                None => "",
            };
            Ok(vec![
                int(relid),
                text(&column.name),
                int(oid_i32(column.ty.oid())?),
                Datum::Int2(column.ty.type_size()),
                Datum::Int2(attnum),
                int(catalog_typmod(column.ty)),
                Datum::Int2(i16::from(matches!(column.ty, ColumnType::Array(_)))),
                Datum::Bool(column.ty.type_size() > 0),
                Datum::InternalChar(b'i'),
                Datum::InternalChar(attribute_storage(column.ty).as_bytes()[0]),
                Datum::InternalChar(b'\0'),
                Datum::Bool(column.not_null),
                // `atthasdef` means "this column has a `pg_attrdef` row", and a
                // generated column has one: its expression is stored there, and
                // psql's `\d` reads the body of `generated always as (…)`
                // through this flag.
                Datum::Bool(column.default.is_some() || column.generated.is_some()),
                Datum::Bool(false),
                Datum::InternalChar(identity.as_bytes().first().copied().unwrap_or(b'\0')),
                Datum::InternalChar(
                    column
                        .attgenerated()
                        .as_bytes()
                        .first()
                        .copied()
                        .unwrap_or(b'\0'),
                ),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Int2(0),
                int(column_collation_oid(column)),
                Datum::Int2(-1),
                acl.of(&table.name, &column.name),
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ])
        })
        .collect()
}

/// `pg_attribute.atttypmod`. [`ColumnType::typmod`] covers the string types;
/// `numeric(p, s)` needs PostgreSQL's packed `((p << 16) | s) + 4` too, because
/// `format_type(atttypid, atttypmod)` reconstructs `numeric(10,2)` from exactly
/// that word. That is how `\d` and every ORM print a column's type.
pub(crate) fn catalog_typmod(ty: ColumnType) -> i32 {
    match ty {
        ColumnType::Numeric(Some(typmod)) => {
            (i32::from(typmod.precision) << 16 | i32::from(typmod.scale)) + 4
        }
        other => other.typmod(),
    }
}

/// `attstorage`: the storage class the column's type declares, which is what
/// `\d+` prints in its Storage column.
///
/// This is `pg_type.typstorage`, read off the pinned 18.4 catalog rather than
/// inferred from whether a type looks fixed-length. Two groups do not follow
/// that intuition: `inet` and `cidr` are `main` alongside `numeric`, not
/// `plain`; and of the geometric types only `point`, `box`, `circle`, `line`
/// and `lseg` are `plain`, while `path` and `polygon` are varlena and so
/// `extended`. Arrays are `extended` except `int2vector` and `oidvector`.
pub(crate) fn attribute_storage(ty: ColumnType) -> &'static str {
    use ColumnType as C;
    match ty {
        // Fixed-length, stored inline and never toasted.
        C::Bool
        | C::Int2
        | C::Int4
        | C::Int8
        | C::Float4
        | C::Float8
        | C::Date
        | C::Time
        | C::Timetz
        | C::Timestamp
        | C::Timestamptz
        | C::Interval
        | C::Temporal(_, _)
        | C::Uuid
        | C::Money
        // One byte, pass-by-value: `pg_type.typstorage` for OID 18 is `p`,
        // where `character(n)` two arms down is `x`.
        | C::InternalChar
        | C::Oid
        | C::Xid
        | C::Xid8
        | C::Cid
        | C::Tid
        | C::PgLsn
        | C::MacAddr
        | C::MacAddr8
        | C::Point
        | C::Box
        | C::Circle
        | C::Line
        | C::Lseg
        | C::TsQuery
        | C::OidVector
        | C::Int2Vector
        | C::Regclass
        | C::Regtype
        | C::Regprocedure
        | C::Regnamespace
        | C::Regproc
        | C::Regoper
        | C::Regoperator
        | C::Regconfig
        | C::Regdictionary
        | C::Regrole
        | C::Regcollation => "p",
        // Compressible but kept in the main table before it is toasted out.
        C::Numeric(_) | C::Inet | C::Cidr => "m",
        // Varlena: compressible and toastable.
        C::Text
        | C::Varchar(_)
        | C::Char(_)
        | C::Bytea
        | C::Json
        | C::Jsonb
        | C::Xml
        | C::JsonPath
        | C::TsVector
        | C::Bit(_)
        | C::VarBit(_)
        | C::Path
        | C::Polygon
        // Both snapshot types are `x` in the pinned catalog, and `d`-aligned
        // rather than `i`-aligned, because their running list is 64-bit.
        | C::PgSnapshot
        | C::TxidSnapshot
        | C::Array(_)
        | C::Record(_)
        | C::Range(_)
        | C::Multirange(_) => "x",
        // An enum is a fixed four-byte oid; a domain takes its base type's
        // class, which is the rule PostgreSQL's `CREATE DOMAIN` copies.
        C::Enum(_) => "p",
        C::Domain(domain) => attribute_storage(*domain.base),
        // `LIKE = T` copies `T`'s `typlen`/`typbyval`/`typalign`, and
        // `typstorage` follows the same physical layout.
        C::Base(base) => attribute_storage(*base.representation),
    }
}

/// `pg_attribute.attcollation` for one catalog column: the oid of the collation
/// its `COLLATE` clause named, or — with no clause written — whatever the type
/// alone implies.
///
/// `\d` prints its Collation column exactly when `attcollation` differs from the
/// type's `typcollation`, so a column that wrote no clause has to keep reporting
/// the type's own collation and a column that wrote `COLLATE "default"` has to
/// report it too: `PostgreSQL` prints nothing for either.
pub(crate) fn column_collation_oid(column: &Column) -> i32 {
    match column.collation.as_deref() {
        None => text_collation_oid(column.ty),
        Some(name) => {
            crate::catalog_rel::collation_oid(name).unwrap_or_else(|| text_collation_oid(column.ty))
        }
    }
}

/// `attcollation`: the database default collation for a collatable type, 0 for
/// everything else, the exact test `\d`'s collation column makes.
pub(crate) fn text_collation_oid(ty: ColumnType) -> i32 {
    if matches!(
        ty,
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_)
    ) {
        crate::catalog_rel::DEFAULT_COLLATION_OID
    } else {
        0
    }
}

/// `pg_type.typcollation` for a built-in type oid: the same database default
/// collation [`text_collation_oid`] gives a column of that type, so `\d`'s
/// `attcollation <> typcollation` test answers false for an uncustomized column.
pub(crate) fn builtin_type_collation_oid(oid: i32) -> i32 {
    let collatable = matches!(
        u32::try_from(oid),
        Ok(crabka_pgtypes::oids::TEXT
            | crabka_pgtypes::oids::VARCHAR
            | crabka_pgtypes::oids::BPCHAR)
    );
    if collatable {
        crate::catalog_rel::DEFAULT_COLLATION_OID
    } else {
        0
    }
}

pub(crate) fn pg_type_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let proc_oids = builtin_proc_oids()?;
    let mut rows: Vec<Vec<Datum>> = builtin_type_rows()
        .iter()
        .map(|ty| {
            // Every exposed built-in — scalar or array — is a base type with
            // no domain base type. Range and multirange rows use their own
            // `typtype`, matching PostgreSQL's catalogue.
            let typtype = if ty.category == "R" && ty.name.ends_with("multirange") {
                "m"
            } else if ty.category == "R" {
                "r"
            } else {
                "b"
            };
            pg_type_row(
                PgTypeRow {
                    oid: ty.oid,
                    name: ty.name,
                    namespace: PG_CATALOG_NAMESPACE_OID,
                    len: i32::from(ty.len),
                    category: ty.category,
                    typtype,
                    typrelid: 0,
                    typelem: ty.elem,
                    typarray: ty.array,
                    typbasetype: 0,
                    typcollation: builtin_type_collation_oid(ty.oid),
                    domain_base: None,
                    range_align: match ty.name {
                        "tsrange" | "tstzrange" | "int8range" => Some("d"),
                        _ => None,
                    },
                },
                &proc_oids,
            )
        })
        .collect();
    rows.extend(user_type_rows(catalog_kv, &proc_oids)?);
    Ok(rows)
}

fn builtin_proc_oids() -> Result<BTreeMap<String, i32>, ExecError> {
    Ok(crate::routine::builtin_pg_proc_rows()?
        .into_iter()
        .filter_map(|row| match (row.first(), row.get(1)) {
            (Some(Datum::Int4(oid)), Some(Datum::Text(name))) => Some((name.clone(), *oid)),
            _ => None,
        })
        .collect())
}

/// The 32 physical columns PostgreSQL exposes from `pg_type`.
///
/// The executor used to publish only the fields its early driver probes used.
/// That made the catalog unlike a catalog table: upstream's own type sanity
/// checks could not bind ordinary fields such as `typisdefined` or `typinput`.
/// Keep the row construction here, beside the data source, so every built-in
/// and user type remains the same width as the virtual relation.
struct PgTypeRow<'a> {
    oid: i32,
    name: &'a str,
    namespace: i32,
    len: i32,
    category: &'a str,
    typtype: &'a str,
    typrelid: i32,
    typelem: i32,
    typarray: i32,
    typbasetype: i32,
    typcollation: i32,
    domain_base: Option<&'a str>,
    range_align: Option<&'a str>,
}

fn pg_type_row(row: PgTypeRow<'_>, proc_oids: &BTreeMap<String, i32>) -> Vec<Datum> {
    let typbyval = matches!(row.len, 1 | 2 | 4 | 8);
    let typalign = row.range_align.unwrap_or(match row.len {
        1 => "c",
        2 => "s",
        8 => "d",
        _ => "i",
    });
    let typstorage = if row.len < 0 { "x" } else { "p" };
    let routines = pg_type_routines(&row, proc_oids);
    vec![
        oid(row.oid),
        text(row.name),
        oid(row.namespace),
        oid(10),
        Datum::Int2(row.len.try_into().expect("pg_type typlen must fit in int2")),
        Datum::Bool(typbyval),
        Datum::InternalChar(row.typtype.as_bytes()[0]),
        Datum::InternalChar(row.category.as_bytes()[0]),
        Datum::Bool(false),
        Datum::Bool(true),
        Datum::InternalChar(b','),
        oid(row.typrelid),
        routines[0].clone(),
        oid(row.typelem),
        oid(row.typarray),
        routines[1].clone(),
        routines[2].clone(),
        routines[3].clone(),
        routines[4].clone(),
        routines[5].clone(),
        routines[6].clone(),
        routines[7].clone(),
        Datum::InternalChar(typalign.as_bytes()[0]),
        Datum::InternalChar(typstorage.as_bytes()[0]),
        Datum::Bool(false),
        oid(row.typbasetype),
        int(-1),
        int(0),
        oid(row.typcollation),
        Datum::Null,
        Datum::Null,
        Datum::Null,
    ]
}

/// The built-in procedure fixture is also PostgreSQL's authoritative OID
/// source for the regproc links in `pg_type`. Most I/O function names are
/// mechanical; the underscore fallback covers the handful of families such as
/// `timestamp_in` and `bit_in`.
fn pg_type_routines(row: &PgTypeRow<'_>, proc_oids: &BTreeMap<String, i32>) -> [Datum; 8] {
    let routine = |name: &str| {
        proc_oids
            .get(name)
            .map_or_else(absent_regproc, |oid| regproc(*oid, name))
    };
    let named = |type_name: &str, suffix: &str| {
        let plain = format!("{type_name}{suffix}");
        proc_oids.get(&plain).map_or_else(
            || {
                let underscored = format!("{type_name}_{suffix}");
                routine(&underscored)
            },
            |oid| regproc(*oid, &plain),
        )
    };
    let is_array = row.name.starts_with('_') || matches!(row.name, "int2vector" | "oidvector");
    if is_array {
        return [
            routine("array_subscript_handler"),
            routine("array_in"),
            routine("array_out"),
            routine("array_recv"),
            routine("array_send"),
            absent_regproc(),
            absent_regproc(),
            routine("array_typanalyze"),
        ];
    }
    if row.typtype == "e" {
        return [
            absent_regproc(),
            routine("enum_in"),
            routine("enum_out"),
            routine("enum_recv"),
            routine("enum_send"),
            absent_regproc(),
            absent_regproc(),
            absent_regproc(),
        ];
    }
    if row.typtype == "c" {
        return [
            absent_regproc(),
            routine("record_in"),
            routine("record_out"),
            routine("record_recv"),
            routine("record_send"),
            absent_regproc(),
            absent_regproc(),
            absent_regproc(),
        ];
    }
    if row.typtype == "d" {
        return [
            absent_regproc(),
            routine("domain_in"),
            row.domain_base
                .map_or_else(absent_regproc, |base| named(base, "out")),
            routine("domain_recv"),
            row.domain_base
                .map_or_else(absent_regproc, |base| named(base, "send")),
            absent_regproc(),
            absent_regproc(),
            absent_regproc(),
        ];
    }
    let family = if row.typtype == "m" {
        Some("multirange")
    } else if row.typtype == "r" {
        Some("range")
    } else {
        None
    };
    let io = |suffix: &str| {
        family.map_or_else(
            || named(pg_type_routine_stem(row.name), suffix),
            |name| routine(&format!("{name}_{suffix}")),
        )
    };
    [
        absent_regproc(),
        io("in"),
        io("out"),
        io("recv"),
        io("send"),
        named(pg_type_routine_stem(row.name), "typmodin"),
        named(pg_type_routine_stem(row.name), "typmodout"),
        if row.typtype == "r" {
            routine("range_typanalyze")
        } else {
            absent_regproc()
        },
    ]
}

fn pg_type_routine_stem(name: &str) -> &str {
    match name {
        "money" => "cash",
        "polygon" => "poly",
        _ => name,
    }
}

fn regproc(oid: i32, name: &str) -> Datum {
    Datum::Regclass(crabka_pgtypes::RegclassValue::resolved(oid, name))
}

fn absent_regproc() -> Datum {
    Datum::Regclass(crabka_pgtypes::RegclassValue::unresolved(0))
}

pub(crate) fn text_search_catalog_rows(
    kv: &dyn Kv,
    kind: crabka_pgparser::ast::TextSearchObjectKind,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crate::text_search_catalog::catalog_rows(kv, kind)?
        .into_iter()
        .map(|(name, base)| {
            let oid = crate::text_search_catalog::object_oid(&name);
            match kind {
                crabka_pgparser::ast::TextSearchObjectKind::Configuration => vec![
                    Datum::Int4(oid),
                    Datum::Text(name),
                    Datum::Int4(PG_CATALOG_NAMESPACE_OID),
                    Datum::Int4(10),
                    Datum::Int4(3722),
                ],
                crabka_pgparser::ast::TextSearchObjectKind::Dictionary => vec![
                    Datum::Int4(oid),
                    Datum::Text(name),
                    Datum::Int4(PG_CATALOG_NAMESPACE_OID),
                    Datum::Int4(10),
                    Datum::Int4(3727),
                    if base.is_empty() {
                        Datum::Null
                    } else {
                        Datum::Text(base)
                    },
                ],
            }
        })
        .collect())
}

/// The `pg_type` rows of the `CREATE TYPE`/`CREATE DOMAIN` types.
///
/// `typrelid` of a composite is the derived `pg_class` oid its attributes hang
/// off (`pg_attribute` uses the same derivation), and `typbasetype` of a domain
/// is the base type's oid — the two columns `\d` and every driver's type
/// introspection walk.
pub(crate) fn user_type_rows(
    catalog_kv: &dyn Kv,
    proc_oids: &BTreeMap<String, i32>,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgtypes::usertype;
    let mut rows = Vec::new();
    for ty in crabka_pgcatalog::list_user_types(catalog_kv)? {
        let column_type = ty.column_type();
        let (typrelid, typbasetype, category) = match &ty.body {
            usertype::UserTypeBody::Composite(_) => (
                i32::try_from(usertype::composite_relation_oid(ty.oid)).unwrap_or(0),
                0,
                "C",
            ),
            usertype::UserTypeBody::Enum(_) => (0, 0, "E"),
            usertype::UserTypeBody::Range(_) => (0, 0, "R"),
            usertype::UserTypeBody::Domain(domain) => (
                0,
                i32::try_from(domain.base.oid()).unwrap_or(0),
                builtin_type_category(domain.base),
            ),
            // `TypeShellMake` writes the pseudo-type class and a four-byte
            // `typlen` — not 0, which `type_sanity` reads as a corrupt row.
            usertype::UserTypeBody::Shell => (0, 0, "P"),
            usertype::UserTypeBody::Base(base) => (0, 0, base.category.as_str()),
        };
        // A shell is the one user type with no `ColumnType`, and `TypeShellMake`
        // gives it `sizeof(int32)` regardless.
        let shell_typlen = 4;
        rows.push(pg_type_row(
            PgTypeRow {
                oid: i32::try_from(ty.oid).unwrap_or(0),
                name: &ty.name,
                namespace: crate::catalog_rel::namespace_oid(&ty.schema),
                len: column_type.map_or(shell_typlen, |ty| i32::from(ty.type_size())),
                category,
                typtype: ty.typtype(),
                typrelid,
                typelem: 0,
                typarray: 0,
                typbasetype,
                typcollation: column_type.map_or(0, text_collation_oid),
                domain_base: match &ty.body {
                    usertype::UserTypeBody::Domain(domain) => builtin_type_name(domain.base.oid()),
                    _ => None,
                },
                range_align: ty.range().map(|range| match range.subtype.type_size() {
                    8 => "d",
                    _ => "i",
                }),
            },
            proc_oids,
        ));
        if let (Some((schema, name)), Some(multirange)) =
            (ty.multirange_identity(), ty.multirange_type())
        {
            rows.push(pg_type_row(
                PgTypeRow {
                    oid: i32::try_from(ty.oid + 3).unwrap_or(0),
                    name: &name,
                    namespace: crate::catalog_rel::namespace_oid(&schema),
                    len: i32::from(multirange.type_size()),
                    category: "R",
                    typtype: "m",
                    typrelid: 0,
                    typelem: 0,
                    typarray: 0,
                    typbasetype: 0,
                    typcollation: 0,
                    domain_base: None,
                    range_align: None,
                },
                proc_oids,
            ));
        }
    }
    Ok(rows)
}

pub(crate) fn pg_range_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let proc_oids = builtin_proc_oids()?;
    let routine = |name: &str| {
        proc_oids
            .get(name)
            .map_or_else(absent_regproc, |oid| regproc(*oid, name))
    };
    let mut rows = [
        (
            3904,
            23,
            4451,
            1978,
            Some("int4range_canonical"),
            "int4range_subdiff",
        ),
        (3906, 1700, 4532, 3125, None, "numrange_subdiff"),
        (3908, 1114, 4533, 3128, None, "tsrange_subdiff"),
        (3910, 1184, 4534, 3127, None, "tstzrange_subdiff"),
        (
            3912,
            1082,
            4535,
            3122,
            Some("daterange_canonical"),
            "daterange_subdiff",
        ),
        (
            3926,
            20,
            4536,
            3124,
            Some("int8range_canonical"),
            "int8range_subdiff",
        ),
    ]
    .into_iter()
    .map(|(range, subtype, multirange, subopc, canonical, subdiff)| {
        vec![
            oid(range),
            oid(subtype),
            oid(multirange),
            oid(0),
            oid(subopc),
            canonical.map_or_else(absent_regproc, routine),
            routine(subdiff),
        ]
    })
    .collect::<Vec<_>>();
    rows.extend(
        crabka_pgcatalog::list_user_types(catalog_kv)?
            .into_iter()
            .filter_map(|ty| {
                let range = ty.range()?;
                Some(vec![
                    oid(i32::try_from(ty.oid).unwrap_or(0)),
                    oid(i32::try_from(range.subtype.oid()).unwrap_or(0)),
                    oid(i32::try_from(ty.oid + 3).unwrap_or(0)),
                    oid(0),
                    oid(0),
                    absent_regproc(),
                    absent_regproc(),
                ])
            }),
    );
    Ok(rows)
}

/// The `pg_type.typcategory` of a built-in type, for the domain rows that
/// inherit their base type's category.
pub(crate) fn builtin_type_category(base: crabka_pgtypes::ColumnType) -> &'static str {
    builtin_type_rows()
        .iter()
        .find(|row| u32::try_from(row.oid) == Ok(base.oid()))
        .map_or("U", |row| row.category)
}

/// The one-column OID indexes declared with `DECLARE_UNIQUE_INDEX_PKEY` by the
/// PostgreSQL 18.4 headers for the base catalogs crabka exposes. Their names
/// and oids are catalog identity, so keep the pinned values rather than minting
/// synthetic index ids as user-created indexes do.
pub(crate) struct BuiltinCatalogOidIndex {
    pub(crate) table: &'static str,
    pub(crate) name: &'static str,
    pub(crate) oid: i32,
}

pub(crate) const BUILTIN_CATALOG_OID_INDEXES: &[BuiltinCatalogOidIndex] = &[
    BuiltinCatalogOidIndex {
        table: "pg_namespace",
        name: "pg_namespace_oid_index",
        oid: 2685,
    },
    BuiltinCatalogOidIndex {
        table: "pg_class",
        name: "pg_class_oid_index",
        oid: 2662,
    },
    BuiltinCatalogOidIndex {
        table: "pg_type",
        name: "pg_type_oid_index",
        oid: 2703,
    },
    BuiltinCatalogOidIndex {
        table: "pg_ts_config",
        name: "pg_ts_config_oid_index",
        oid: 3712,
    },
    BuiltinCatalogOidIndex {
        table: "pg_ts_dict",
        name: "pg_ts_dict_oid_index",
        oid: 3605,
    },
    BuiltinCatalogOidIndex {
        table: "pg_am",
        name: "pg_am_oid_index",
        oid: 2652,
    },
    BuiltinCatalogOidIndex {
        table: "pg_amop",
        name: "pg_amop_oid_index",
        oid: 2756,
    },
    BuiltinCatalogOidIndex {
        table: "pg_amproc",
        name: "pg_amproc_oid_index",
        oid: 2757,
    },
    BuiltinCatalogOidIndex {
        table: "pg_attrdef",
        name: "pg_attrdef_oid_index",
        oid: 2657,
    },
    BuiltinCatalogOidIndex {
        table: "pg_authid",
        name: "pg_authid_oid_index",
        oid: 2677,
    },
    BuiltinCatalogOidIndex {
        table: "pg_cast",
        name: "pg_cast_oid_index",
        oid: 2660,
    },
    BuiltinCatalogOidIndex {
        table: "pg_collation",
        name: "pg_collation_oid_index",
        oid: 3085,
    },
    BuiltinCatalogOidIndex {
        table: "pg_constraint",
        name: "pg_constraint_oid_index",
        oid: 2667,
    },
    BuiltinCatalogOidIndex {
        table: "pg_conversion",
        name: "pg_conversion_oid_index",
        oid: 2670,
    },
    BuiltinCatalogOidIndex {
        table: "pg_database",
        name: "pg_database_oid_index",
        oid: 2672,
    },
    BuiltinCatalogOidIndex {
        table: "pg_enum",
        name: "pg_enum_oid_index",
        oid: 3502,
    },
    BuiltinCatalogOidIndex {
        table: "pg_event_trigger",
        name: "pg_event_trigger_oid_index",
        oid: 3468,
    },
    BuiltinCatalogOidIndex {
        table: "pg_extension",
        name: "pg_extension_oid_index",
        oid: 3080,
    },
    BuiltinCatalogOidIndex {
        table: "pg_language",
        name: "pg_language_oid_index",
        oid: 2682,
    },
    BuiltinCatalogOidIndex {
        table: "pg_policy",
        name: "pg_policy_oid_index",
        oid: 3257,
    },
    BuiltinCatalogOidIndex {
        table: "pg_opclass",
        name: "pg_opclass_oid_index",
        oid: 2687,
    },
    BuiltinCatalogOidIndex {
        table: "pg_opfamily",
        name: "pg_opfamily_oid_index",
        oid: 2755,
    },
    BuiltinCatalogOidIndex {
        table: "pg_operator",
        name: "pg_operator_oid_index",
        oid: 2688,
    },
    BuiltinCatalogOidIndex {
        table: "pg_proc",
        name: "pg_proc_oid_index",
        oid: 2690,
    },
    BuiltinCatalogOidIndex {
        table: "pg_publication",
        name: "pg_publication_oid_index",
        oid: 6110,
    },
    BuiltinCatalogOidIndex {
        table: "pg_publication_namespace",
        name: "pg_publication_namespace_oid_index",
        oid: 6238,
    },
    BuiltinCatalogOidIndex {
        table: "pg_publication_rel",
        name: "pg_publication_rel_oid_index",
        oid: 6112,
    },
    BuiltinCatalogOidIndex {
        table: "pg_rewrite",
        name: "pg_rewrite_oid_index",
        oid: 2692,
    },
    BuiltinCatalogOidIndex {
        table: "pg_statistic_ext",
        name: "pg_statistic_ext_oid_index",
        oid: 3380,
    },
    BuiltinCatalogOidIndex {
        table: "pg_tablespace",
        name: "pg_tablespace_oid_index",
        oid: 2697,
    },
    BuiltinCatalogOidIndex {
        table: "pg_trigger",
        name: "pg_trigger_oid_index",
        oid: 2702,
    },
];

pub(crate) fn builtin_catalog_oid_index(table: &str) -> Option<&'static BuiltinCatalogOidIndex> {
    BUILTIN_CATALOG_OID_INDEXES
        .iter()
        .find(|index| index.table == table)
}

pub(crate) fn builtin_catalog_index_table(index: &BuiltinCatalogOidIndex) -> Table {
    let oid_column = virtual_catalog_table(index.table)
        .columns
        .into_iter()
        .find(|column| column.name == "oid")
        .expect("catalog oid index refers to a catalog with an oid column");
    Table {
        id: u32::try_from(index.oid).expect("built-in index oid is positive"),
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: crabka_pgcatalog::RelationName::new(crate::search_path::PG_CATALOG, index.name),
        columns: vec![oid_column],
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    }
}

pub(crate) fn pg_index_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = crabka_pgcatalog::list_indexes(catalog_kv)?
        .into_iter()
        .map(|index| {
            let table = crabka_pgcatalog::get_table(catalog_kv, &index.table)?;
            // An expression key has no table column to point at: PostgreSQL
            // writes 0 in `indkey` for it and carries the expression itself in
            // `indexprs`, in key order. Looking one up as a column name instead
            // fails every read of the whole catalog, because one unreadable
            // index poisons the projection for every other.
            let mut expressions = Vec::new();
            let mut indkey = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                if let Some(source) = crabka_pgcatalog::index_key_expression(column) {
                    expressions.push(source);
                    indkey.push(Datum::Int4(0));
                    continue;
                }
                let attnum = table
                    .column_index(column)
                    .and_then(|idx| i32::try_from(idx + 1).ok())
                    .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
                indkey.push(Datum::Int4(attnum));
            }
            // `indexprs` is a node tree in PostgreSQL and text here, which is
            // what `pg_get_expr` reports either way; the list separator is the
            // one `pg_get_expr(indexprs, indrelid)` prints for a multi-key
            // index.
            let indexprs = if expressions.is_empty() {
                Datum::Null
            } else {
                text(&expressions.join(", "))
            };
            let natts = i16::try_from(index.columns.len())
                .map_err(|_| ExecError::Unsupported("indnatts exceeds int2 range".into()))?;
            Ok(vec![
                int(catalog_index_oid(index.id)?),
                int(crate::catalog_rel::table_relation_oid(index.table_id)?),
                Datum::Int2(natts),
                Datum::Int2(natts),
                Datum::Bool(index.unique),
                Datum::Bool(false),
                // The catalog knows which index backs the primary key; ORMs
                // introspecting for upserts key off exactly this column.
                Datum::Bool(
                    index.constraint == Some(crabka_pgcatalog::IndexConstraint::PrimaryKey),
                ),
                // `indisexclusion` covers a `WITHOUT OVERLAPS` key too: it is
                // an exclusion constraint that also happens to be catalogued as
                // a primary key or a unique constraint.
                Datum::Bool(index.exclusion_operators().is_some()),
                // `indimmediate`: false for a `DEFERRABLE` key, whose check
                // waits for the end of the statement at the earliest. This is
                // the column that says an index is not continuously unique,
                // which is why `REPLICA IDENTITY USING INDEX` and a foreign
                // key's referent both read it rather than `pg_constraint`.
                Datum::Bool(!index.deferral.is_deferrable()),
                // `indisclustered`: the index a bare `CLUSTER <table>` reorders
                // the heap by, set by `CLUSTER … USING` and by
                // `ALTER TABLE … CLUSTER ON`.
                Datum::Bool(index.clustered),
                // Every crabka index is valid, ready and live the moment it is
                // in the catalog: there is no concurrent-build state.
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::OidVector(crabka_pgtypes::ArrayValue::with_dims(
                    crabka_pgtypes::ElemType::Int4,
                    indkey,
                    vec![crabka_pgtypes::ArrayDim::new(0, i32::from(natts))],
                )),
                // indcollation, indclass, indoption.
                Datum::Null,
                Datum::Null,
                Datum::Null,
                indexprs,
                // `indpred`: crabka has no partial indexes — `CREATE INDEX …
                // WHERE` is refused — so a stored index is never predicated.
                Datum::Null,
            ])
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    for index in BUILTIN_CATALOG_OID_INDEXES {
        let table = virtual_catalog_table(index.table);
        let attnum = table
            .column_index("oid")
            .and_then(|index| i16::try_from(index + 1).ok())
            .ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "catalog {} has no addressable oid column",
                    index.table
                ))
            })?;
        rows.push(vec![
            int(index.oid),
            int(virtual_relation_oid(index.table)),
            Datum::Int2(1),
            Datum::Int2(1),
            Datum::Bool(true),
            Datum::Bool(false),
            Datum::Bool(true),
            Datum::Bool(false),
            Datum::Bool(true),
            Datum::Bool(false),
            Datum::Bool(true),
            Datum::Bool(false),
            Datum::Bool(true),
            Datum::Bool(true),
            Datum::Bool(false),
            Datum::OidVector(crabka_pgtypes::ArrayValue::with_dims(
                crabka_pgtypes::ElemType::Int4,
                vec![Datum::Int4(i32::from(attnum))],
                vec![crabka_pgtypes::ArrayDim::new(0, 1)],
            )),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]);
    }
    Ok(rows)
}

pub(crate) fn pg_settings_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    crate::session::guc_settings_runtime()?
        .into_iter()
        .map(|setting| {
            let optional = |value: Option<&String>| value.map_or(Datum::Null, |value| text(value));
            Ok(vec![
                text(&setting.name),
                text(&setting.value),
                optional(setting.unit.as_ref()),
                text("Client Connection Defaults / Statement Behavior"),
                text("Crabka session parameter"),
                text(&setting.context),
                text(&setting.vartype),
                text("session"),
                optional(setting.min_val.as_ref()),
                optional(setting.max_val.as_ref()),
                optional(setting.enumvals.as_ref()),
                text(&setting.boot_val),
                text(&setting.reset_val),
                Datum::Bool(false),
            ])
        })
        .collect()
}

/// S2: `pg_catalog.pg_prepared_statements` over the session's prepared
/// statements. `parameter_types`/`result_types` are rendered as `PostgreSQL`
/// renders a `regtype[]` literal.
pub(crate) fn pg_prepared_statement_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crate::session::prepared_statement_runtime()?
        .into_iter()
        .map(|prepared| {
            vec![
                text(&prepared.name),
                text(&prepared.statement),
                Datum::Null,
                text(&prepared.parameter_types),
                text(&prepared.result_types),
                Datum::Bool(prepared.from_sql),
                Datum::Int8(0),
                Datum::Int8(1),
            ]
        })
        .collect())
}

/// Resolve every member a `GRANT`/`REVOKE ROLE` names and check that the
/// session may move membership in every role it hands out.
///
/// The names come first and the rights come second, which is the order
/// `PostgreSQL` reports them in: it resolves the grantees, then resolves and
/// checks each granted role in turn, so a statement naming a role that does not
/// exist says so rather than reporting a denial for a name that means nothing.
/// Every name is checked before any membership is written, so a statement that
/// fails on its second role writes nothing for its first.
///
/// # Errors
///
/// Returns 42704 for a member or role no role holds — `PUBLIC` included, which
/// is a grantee of privileges but never a member of anything — or
/// storage/corruption errors from the catalog KV seam.
pub(crate) fn require_role_memberships(
    kv: &dyn Kv,
    fctx: ForeignCtx<'_>,
    roles: &[String],
    members: &[crabka_pgparser::ast::RoleSpec],
    direction: crate::privilege::RoleGrant,
) -> Result<Vec<String>, ExecError> {
    // `PUBLIC` has no membership to move, on either side: `GRANT r TO PUBLIC`
    // and `GRANT PUBLIC TO r` are both 42704 in PostgreSQL, even though
    // `GRANT SELECT … TO PUBLIC` is the ordinary way to open a relation to
    // everyone. Membership needs a role with a record, and it has none.
    let holds_membership = |name: &str| -> Result<bool, ExecError> {
        Ok(name != crabka_pgcatalog::PUBLIC_ROLE && crabka_pgcatalog::role_is_nameable(kv, name)?)
    };
    let mut resolved = Vec::with_capacity(members.len());
    for member in members {
        let member = role_spec_name(member, fctx);
        if !holds_membership(member)? {
            return Err(undefined_role(member));
        }
        resolved.push(member.to_string());
    }
    for role in roles {
        if !holds_membership(role)? {
            return Err(undefined_role(role));
        }
        crate::privilege::require_role_grant(kv, fctx.effective_role(), role, direction)?;
    }
    Ok(resolved)
}

/// Fold a written `CREATE`/`ALTER ROLE` option list onto stored attributes,
/// returning the resulting login flag. An option the statement did not write
/// keeps its current value, which is what `ALTER ROLE … WITH SUPERUSER` means.
pub(crate) fn apply_role_options(
    attributes: &mut crabka_pgcatalog::RoleAttributes,
    can_login: bool,
    options: crabka_pgparser::ast::RoleOptions,
) -> bool {
    use crabka_pgcatalog::RoleAttribute;
    for (attribute, written) in [
        (RoleAttribute::Superuser, options.superuser),
        (RoleAttribute::Inherit, options.inherit),
        (RoleAttribute::CreateRole, options.createrole),
        (RoleAttribute::CreateDb, options.createdb),
        (RoleAttribute::Replication, options.replication),
        (RoleAttribute::BypassRls, options.bypassrls),
    ] {
        if let Some(value) = written {
            attributes.set(attribute, value);
        }
    }
    options.login.unwrap_or(can_login)
}

pub(crate) fn pg_roles_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgcatalog::RoleAttribute;
    let oids = crate::catalog_rel::role_oids(catalog_kv)?;
    Ok(crabka_pgcatalog::list_roles(catalog_kv)?
        .into_iter()
        .map(|role| {
            let bootstrap = role.name == crate::catalog_fn::OBJECT_OWNER;
            let attributes = role.attributes;
            vec![
                text(&role.name),
                Datum::Bool(bootstrap || attributes.has(RoleAttribute::Superuser)),
                Datum::Bool(attributes.has(RoleAttribute::Inherit)),
                Datum::Bool(bootstrap || attributes.has(RoleAttribute::CreateRole)),
                Datum::Bool(bootstrap || attributes.has(RoleAttribute::CreateDb)),
                Datum::Bool(role.can_login),
                Datum::Bool(attributes.has(RoleAttribute::Replication)),
                int(-1),
                // PostgreSQL blanks the password in `pg_roles` (only
                // `pg_authid` holds it, and only a superuser may read that).
                text("********"),
                Datum::Null,
                Datum::Bool(bootstrap || attributes.has(RoleAttribute::BypassRls)),
                Datum::Null,
                int(oids.get(&role.name).copied().unwrap_or(0)),
            ]
        })
        .collect())
}

pub(crate) fn pg_user_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_roles(catalog_kv)?
        .into_iter()
        .filter(|role| role.can_login)
        .map(|role| vec![text(&role.name), Datum::Bool(false), Datum::Bool(false)])
        .collect())
}

/// Every virtual relation, `exec`'s starter set followed by the F-2
/// introspection surface. `pg_class`/`pg_attribute` describe themselves through
/// this list, so a relation missing from it is invisible to `\d`.
pub(crate) fn virtual_table_names() -> &'static [&'static str] {
    static NAMES: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
        let mut names = vec![
            "pg_namespace",
            "pg_class",
            "pg_attribute",
            "pg_type",
            "pg_ts_config",
            "pg_ts_dict",
            "pg_range",
            "pg_index",
            "pg_settings",
            "pg_prepared_statements",
            "pg_roles",
            "pg_user",
            "information_schema.schemata",
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.triggers",
            "information_schema.triggered_update_columns",
        ];
        names.extend_from_slice(crate::catalog_rel::relation_names());
        names
    });
    &NAMES
}

pub(crate) fn virtual_relation_name(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(_, relation)| relation)
}

/// The schema a synthesised catalog relation lives in: `information_schema`
/// for the SQL-standard views, `pg_catalog` for everything else.
pub(crate) fn virtual_relation_schema(name: &str) -> &'static str {
    if name.starts_with("information_schema.") {
        "information_schema"
    } else {
        crate::search_path::PG_CATALOG
    }
}

pub(crate) fn virtual_relation_namespace_oid(name: &str) -> i32 {
    if name.starts_with("information_schema.") {
        INFORMATION_SCHEMA_NAMESPACE_OID
    } else {
        PG_CATALOG_NAMESPACE_OID
    }
}

/// Resolve a `regclass` relation name to its `pg_class` oid: virtual catalog
/// relations use their fixed oids; user tables use their catalog table id (the
/// same value `pg_class_rows` reports). An optional `pg_catalog.` / `public.`
/// qualifier is accepted like PostgreSQL's search path would.
///
/// # Errors
///
/// Propagates the catalog's undefined-table error (42P01) for an unknown
/// relation name, matching PostgreSQL's `relation "..." does not exist`.
pub(crate) fn resolve_regclass(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    name: &str,
) -> Result<i32, ExecError> {
    crate::catalog_fn::resolve_relation_in_scope(catalog_kv, scope, name)
}

/// The `regclass` value for a relation oid: the oid paired with the name
/// `regclassout` prints for it. An oid no relation has is not an error in
/// PostgreSQL. It keeps the fallback rendering, `-` for `InvalidOid` and the
/// bare number otherwise, which [`RegclassValue::unresolved`] supplies.
///
/// `scope` is the session's, because `regclassout` schema-qualifies exactly
/// when an unqualified reference would miss the relation, and that question has
/// no answer without a search path.
pub(crate) fn regclass_by_oid(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    oid: i32,
) -> Result<crabka_pgtypes::RegclassValue, ExecError> {
    Ok(
        crate::catalog_fn::relation_name_by_oid(catalog_kv, scope, oid)?.map_or_else(
            || crabka_pgtypes::RegclassValue::unresolved(oid),
            |name| crabka_pgtypes::RegclassValue::resolved(oid, &name),
        ),
    )
}

/// Which `reg*` type a column holds, if any — the type itself, or a domain over
/// it, whose values *are* the base type's values.
pub(crate) fn holds_reg(ty: ColumnType) -> Option<crate::reg_fn::RegKind> {
    match ty {
        ColumnType::Domain(domain) => holds_reg(*domain.base),
        _ => crate::reg_fn::RegKind::of(ty),
    }
}

/// Re-attach the object name to every `reg*` value a scan just decoded.
///
/// The row encoding stores a `regclass` as its bare oid, which is all
/// PostgreSQL keeps on disk too, so a decoded value arrives as a
/// `Datum::Int4`. PostgreSQL
/// consults the catalog in `regclassout`; crabka cannot, because the text
/// encoder and the `→ text` cast both live in a crate with no catalog handle.
/// The scan is the last point where the catalog *is* in scope, so the name is
/// attached here and travels with the value, exactly as the `::regclass` cast
/// arranges for a value that never touched storage.
///
/// Resolving from the catalog on the way out rather than storing the name is
/// what makes an already-stored value follow a `RENAME` and fall back to the
/// bare oid once its relation is dropped, which is what PostgreSQL does.
///
/// [`crate::catalog_fn::relation_name_by_oid`] reads the catalog and then walks
/// the search path to decide whether to qualify, so the lookup is memoized
/// across the scan: a column holding one repeated oid costs one lookup, not one
/// per row. A table with no `regclass` column returns before touching a row.
pub(crate) fn resolve_scanned_regclass(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    table: &crabka_pgcatalog::Table,
    rows: &mut [Vec<Datum>],
) -> Result<(), ExecError> {
    resolve_regclass_at(catalog_kv, scope, &regclass_column_indexes(table, 0), rows)
}

/// The positions of `table`'s `regclass`-valued columns within a scanned row
/// whose first column sits at `offset` — non-zero for a join result, which
/// concatenates one table's columns after another's.
pub(crate) fn regclass_column_indexes(
    table: &crabka_pgcatalog::Table,
    offset: usize,
) -> Vec<(usize, crate::reg_fn::RegKind)> {
    table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| Some((index + offset, holds_reg(column.ty)?)))
        .collect()
}

/// The shared body of [`resolve_scanned_regclass`], over already-located
/// columns.
pub(crate) fn resolve_regclass_at(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    columns: &[(usize, crate::reg_fn::RegKind)],
    rows: &mut [Vec<Datum>],
) -> Result<(), ExecError> {
    if columns.is_empty() {
        return Ok(());
    }
    let mut resolved: HashMap<(i32, crate::reg_fn::RegKind), crabka_pgtypes::RegclassValue> =
        HashMap::new();
    for row in rows {
        for &(index, kind) in columns {
            // A projection that dropped the column, or a NULL, leaves nothing to
            // resolve; a value already carrying its name is left alone.
            let Some(Datum::Int4(oid)) = row.get(index) else {
                continue;
            };
            let oid = *oid;
            let value = match resolved.entry((oid, kind)) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
                std::collections::hash_map::Entry::Vacant(entry) => entry
                    .insert(crate::reg_fn::stored_value(kind, oid, catalog_kv, scope)?)
                    .clone(),
            };
            row[index] = Datum::Regclass(value);
        }
    }
    Ok(())
}

/// PostgreSQL's `regclassin`: an all-digit string is an oid, `-` is
/// `InvalidOid`, and anything else is a relation name the catalog resolves
/// (42P01 when it has none).
pub(crate) fn regclass_from_text(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    text: &str,
) -> Result<Datum, ExecError> {
    let trimmed = text.trim();
    let oid = if trimmed == "-" {
        0
    } else {
        match trimmed.parse::<i32>() {
            Ok(oid) => oid,
            Err(_) => resolve_regclass(catalog_kv, scope, text)?,
        }
    };
    regclass_by_oid(catalog_kv, scope, oid).map(Datum::Regclass)
}

/// The catalog-aware half of a `… :: regclass` cast. `None` for an operand the
/// catalog adds nothing to (NULL, an out-of-range `int8`), which then takes the
/// pure cast in [`crabka_pgtypes::cast`] and its error reporting.
pub(crate) fn regclass_cast(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    value: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let oid = match value {
        Datum::Text(text) => return regclass_from_text(catalog_kv, scope, text).map(Some),
        Datum::Int4(oid) => *oid,
        Datum::Int8(oid) => match i32::try_from(*oid) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        Datum::Regclass(value) => value.oid,
        _ => return Ok(None),
    };
    regclass_by_oid(catalog_kv, scope, oid)
        .map(Datum::Regclass)
        .map(Some)
}

/// The oid of a written type name, as `regtypein` and `parseNameAndArgTypes`
/// both need it: the built-in signature map first, then the user-type registry,
/// and 42704 when neither knows it.
///
/// One resolver rather than three, because `regtype`, `regprocedure`'s argument
/// list and `regoperator`'s operand list must agree about what `int4` and
/// `"char"` mean or a round trip through one of them stops reading back.
///
/// A qualifier is resolved before the type name is, which is where the two
/// error shapes come from: `public.int4` is a missing *type* while
/// `ng_catalog.int4` is a missing *schema*, because `DeconstructQualifiedName`
/// looks the namespace up first and raises 3F000 there.
///
/// # Errors
///
/// 3F000 for a qualifier naming no schema, 42704 `type "…" does not exist`
/// otherwise.
pub(crate) fn resolve_type_name(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    written: &str,
) -> Result<i32, ExecError> {
    let written = written.trim();
    let parts = crate::relname::split_identifier_string(written)
        .filter(|parts| !parts.is_empty())
        .ok_or_else(crate::relname::invalid_name_syntax)?;
    // `DeconstructQualifiedName`, whose two length checks are *hard* errors —
    // the one place `to_regtype` propagates rather than answering NULL.
    let (schema, name) = match parts.as_slice() {
        [name] => (None, name.clone()),
        [schema, name] => (Some(schema.clone()), name.clone()),
        [catalog, schema, name] if *catalog == scope.database => {
            (Some(schema.clone()), name.clone())
        }
        [_, _, _] => {
            return Err(ExecError::FunctionError {
                sqlstate: "0A000",
                message: format!("cross-database references are not implemented: {written}"),
            });
        }
        _ => {
            return Err(ExecError::FunctionError {
                sqlstate: "42601",
                message: format!("improper qualified name (too many dotted names): {written}"),
            });
        }
    };
    if let Some(schema) = &schema
        && !schema_exists(kv, schema)?
    {
        return Err(ExecError::Catalog(
            crabka_pgcatalog::CatalogError::UndefinedSchema(schema.clone()),
        ));
    }
    // crabka declares every type in `pg_catalog`, so a qualifier that names an
    // existing schema other than that one finds nothing — which is exactly what
    // PostgreSQL reports for `public.int4`.
    let qualified_elsewhere = schema
        .as_deref()
        .is_some_and(|schema| schema != "pg_catalog");
    let found = if qualified_elsewhere {
        None
    } else {
        regtype_oid(&name).or_else(|| {
            crabka_pgtypes::usertype::lookup(&name).and_then(|ty| i32::try_from(ty.oid).ok())
        })
    };
    found.ok_or_else(|| {
        let spelled = schema.map_or_else(|| name.clone(), |schema| format!("{schema}.{name}"));
        ExecError::UndefinedObject(format!("type \"{spelled}\" does not exist"))
    })
}

/// Does `name` name a schema? `pg_namespace` is the whole answer, and it is the
/// same list `regnamespace` resolves against.
pub(crate) fn schema_exists(kv: &dyn Kv, name: &str) -> Result<bool, ExecError> {
    Ok(pg_namespace_rows(kv)?
        .iter()
        .any(|row| row.get(1) == Some(&Datum::Text(name.to_string()))))
}

pub(crate) fn regtype_oid(name: &str) -> Option<i32> {
    crate::routine::TYPE_OIDS
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, oid)| *oid)
        // `TYPE_OIDS` lists the scalar spellings. An array is written either
        // `int4[]` or as `pg_type.typname` spells it, `_int4`, and both reach
        // the same type -- so fall back to the ordinary type-name resolver
        // rather than duplicating the array names in the table.
        .or_else(|| {
            // A written type name may spell an array either way. The `[]`
            // suffix is stripped by the parser before a declared type reaches
            // the resolver, but `to_regtype` receives the string as written, so
            // it is handled here rather than in the shared name table.
            let lower = name.to_ascii_lowercase();
            let spelling = lower.trim_end();
            let element = spelling
                .strip_suffix("[]")
                .map(str::trim_end)
                .map(std::borrow::Cow::Borrowed)
                .unwrap_or(std::borrow::Cow::Borrowed(spelling));
            let resolved = crabka_pgtypes::ColumnType::from_builtin_sql_name(&element)?;
            let resolved = if spelling.ends_with("[]") {
                crabka_pgtypes::ColumnType::array_of(resolved)?
            } else {
                resolved
            };
            i32::try_from(resolved.oid()).ok()
        })
}

pub(crate) fn regtype_name(oid: i32) -> String {
    crabka_pgtypes::usertype::lookup_oid(u32::try_from(oid).unwrap_or(0))
        .map(|ty| ty.name.clone())
        .unwrap_or_else(|| {
            let formatted = crate::func::format_type(i64::from(oid), -1);
            if formatted == "-" {
                crate::routine::TYPE_OIDS
                    .iter()
                    .find(|(_, candidate_oid)| *candidate_oid == oid)
                    .map_or_else(|| oid.to_string(), |(name, _)| (*name).to_string())
            } else {
                formatted
            }
        })
}

/// The base-table half of [`resolve_regclass`]: virtual catalog relations and
/// ordinary/foreign tables. [`crate::catalog_fn`] layers views, sequences and
/// indexes, the other three `pg_class` kinds, on top.
///
/// # Errors
///
/// Propagates the catalog's undefined-table error (42P01).
pub(crate) fn resolve_base_relation(
    catalog_kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<i32, ExecError> {
    let key = virtual_lookup_key(name);
    if virtual_table_names().contains(&key.as_str()) {
        return Ok(virtual_relation_oid(&key));
    }
    let table = crabka_pgcatalog::get_table(catalog_kv, name)?;
    crate::catalog_rel::table_relation_oid(table.id)
}

pub(crate) fn virtual_relation_oid(name: &str) -> i32 {
    match name {
        "pg_namespace" => 2615,
        "pg_class" => 1259,
        "pg_attribute" => 1249,
        "pg_type" => 1247,
        "pg_ts_config" => 3602,
        "pg_ts_dict" => 3600,
        "pg_range" => 3541,
        "pg_index" => 2610,
        "pg_settings" => 100_001,
        "pg_prepared_statements" => 100_003,
        "pg_roles" => 1261,
        "pg_user" => 100_002,
        "information_schema.schemata" => 100_010,
        "information_schema.tables" => 100_011,
        "information_schema.columns" => 100_012,
        "information_schema.triggers" => 100_013,
        "information_schema.triggered_update_columns" => 100_014,
        _ => crate::catalog_rel::relation_oid(name),
    }
}

/// The scalar built-in types crabka exposes. `array` is 0 when crabka does not
/// implement that array type; every other one gets a generated array row from
/// [`builtin_type_rows`].
fn scalar_type_rows() -> &'static [BuiltinTypeRow] {
    &[
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::OID as i32,
            name: "oid",
            len: 4,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::OIDARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::XID as i32,
            name: "xid",
            len: 4,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::XIDARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::XID8 as i32,
            name: "xid8",
            len: 8,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::XID8ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::CID as i32,
            name: "cid",
            len: 4,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::CIDARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TID as i32,
            name: "tid",
            len: 6,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::TIDARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::PG_LSN as i32,
            name: "pg_lsn",
            len: 8,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::PG_LSNARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::PG_SNAPSHOT as i32,
            name: "pg_snapshot",
            len: -1,
            category: "U",
            elem: 0,
            array: PG_SNAPSHOT_ARRAY_OID,
        },
        // `txid_snapshot` is a type of its own and not an alias, so it needs a
        // row of its own: a column declared with it reports 2970, and
        // `FigureColname` labels a cast to it `txid_snapshot`.
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TXID_SNAPSHOT as i32,
            name: "txid_snapshot",
            len: -1,
            category: "U",
            elem: 0,
            array: TXID_SNAPSHOT_ARRAY_OID,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::OIDVECTOR as i32,
            name: "oidvector",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::OID as i32,
            array: 1013,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT2VECTOR as i32,
            name: "int2vector",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::INT2 as i32,
            array: INT2VECTOR_ARRAY_OID,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGPROCEDURE as i32,
            name: "regprocedure",
            len: 4,
            category: "N",
            elem: 0,
            array: 2207,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGNAMESPACE as i32,
            name: "regnamespace",
            len: 4,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::REGNAMESPACEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGCLASS as i32,
            name: "regclass",
            len: 4,
            category: "N",
            elem: 0,
            array: 2210,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGPROC as i32,
            name: "regproc",
            len: 4,
            category: "N",
            elem: 0,
            array: 1008,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGOPER as i32,
            name: "regoper",
            len: 4,
            category: "N",
            elem: 0,
            array: 2208,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGOPERATOR as i32,
            name: "regoperator",
            len: 4,
            category: "N",
            elem: 0,
            array: 2209,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGCONFIG as i32,
            name: "regconfig",
            len: 4,
            category: "N",
            elem: 0,
            array: 3735,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGDICTIONARY as i32,
            name: "regdictionary",
            len: 4,
            category: "N",
            elem: 0,
            array: 3770,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGROLE as i32,
            name: "regrole",
            len: 4,
            category: "N",
            elem: 0,
            array: 4097,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGCOLLATION as i32,
            name: "regcollation",
            len: 4,
            category: "N",
            elem: 0,
            array: 4192,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGTYPE as i32,
            name: "regtype",
            len: 4,
            category: "N",
            elem: 0,
            array: 2211,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BOOL as i32,
            name: "bool",
            len: 1,
            category: "B",
            elem: 0,
            array: crabka_pgtypes::oids::BOOLARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BYTEA as i32,
            name: "bytea",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::BYTEAARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT2 as i32,
            name: "int2",
            len: 2,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::INT2ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT8 as i32,
            name: "int8",
            len: 8,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::INT8ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT4 as i32,
            name: "int4",
            len: 4,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::INT4ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TEXT as i32,
            name: "text",
            len: -1,
            category: "S",
            elem: 0,
            array: crabka_pgtypes::oids::TEXTARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BPCHAR as i32,
            name: "bpchar",
            len: -1,
            category: "S",
            elem: 0,
            array: 0,
        },
        // `"char"` is not in the string category with its neighbours: it is
        // `typcategory` Z, the internal-use category, because it is one byte
        // rather than a string. Its `typarray` resolves even though `ElemType`
        // has no variant for it, the position `timetz` and `money` are in.
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::CHAR as i32,
            name: "char",
            len: 1,
            category: "Z",
            elem: 0,
            array: crabka_pgtypes::oids::CHARARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::VARCHAR as i32,
            name: "varchar",
            len: -1,
            category: "S",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::FLOAT4 as i32,
            name: "float4",
            len: 4,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::FLOAT4ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::FLOAT8 as i32,
            name: "float8",
            len: 8,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::FLOAT8ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::POINT as i32,
            name: "point",
            len: 16,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::PATH as i32,
            name: "path",
            len: -1,
            category: "G",
            elem: 0,
            array: 0,
        },
        // The other five geometric types, category 'G' like `point`/`path`.
        // `typelem`/`typarray` stay 0 for the same reason theirs do: crabka
        // builds no array of a geometric type, and `type_sanity` treats a
        // `typarray` pointing at a row that does not exist as a catalog error.
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::LSEG as i32,
            name: "lseg",
            len: 32,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BOX as i32,
            name: "box",
            len: 32,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::POLYGON as i32,
            name: "polygon",
            len: -1,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::LINE as i32,
            name: "line",
            len: 24,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::CIRCLE as i32,
            name: "circle",
            len: 24,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::NUMERIC as i32,
            name: "numeric",
            len: -1,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::NUMERICARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::DATE as i32,
            name: "date",
            len: 4,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::DATEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIME as i32,
            name: "time",
            len: 8,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::TIMEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMETZ as i32,
            name: "timetz",
            len: 12,
            category: "D",
            elem: 0,
            array: TIMETZ_ARRAY_OID,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMESTAMP as i32,
            name: "timestamp",
            len: 8,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::TIMESTAMPARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMESTAMPTZ as i32,
            name: "timestamptz",
            len: 8,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::TIMESTAMPTZARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INTERVAL as i32,
            name: "interval",
            len: 16,
            category: "T",
            elem: 0,
            array: crabka_pgtypes::oids::INTERVALARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::UUID as i32,
            name: "uuid",
            len: 16,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::UUIDARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSON as i32,
            name: "json",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::JSONARRAY as i32,
        },
        // `typcategory` U, like `json` and `jsonb`: `xml` belongs to no family
        // with a preferred type, which is one reason nothing implicitly coerces
        // to it.
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::XML as i32,
            name: "xml",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::XMLARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSONB as i32,
            name: "jsonb",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::JSONBARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSONPATH as i32,
            name: "jsonpath",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::JSONPATHARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSVECTOR as i32,
            name: "tsvector",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::TSVECTORARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSQUERY as i32,
            name: "tsquery",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::TSQUERYARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::MONEY as i32,
            name: "money",
            len: 8,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::MONEYARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BIT as i32,
            name: "bit",
            len: -1,
            category: "V",
            elem: 0,
            array: crabka_pgtypes::oids::BITARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::VARBIT as i32,
            name: "varbit",
            len: -1,
            category: "V",
            elem: 0,
            array: crabka_pgtypes::oids::VARBITARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INET as i32,
            name: "inet",
            len: -1,
            category: "I",
            elem: 0,
            array: crabka_pgtypes::oids::INETARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::CIDR as i32,
            name: "cidr",
            len: -1,
            category: "I",
            elem: 0,
            array: crabka_pgtypes::oids::CIDRARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::MACADDR as i32,
            name: "macaddr",
            len: 6,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::MACADDRARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::MACADDR8 as i32,
            name: "macaddr8",
            len: 8,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::MACADDR8ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT4RANGE as i32,
            name: "int4range",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::INT4RANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::NUMRANGE as i32,
            name: "numrange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::NUMRANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSRANGE as i32,
            name: "tsrange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::TSRANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSTZRANGE as i32,
            name: "tstzrange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::TSTZRANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::DATERANGE as i32,
            name: "daterange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::DATERANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT8RANGE as i32,
            name: "int8range",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::INT8RANGEARRAY as i32,
        },
    ]
}

/// The `pg_type.typname` of an element type's array type, which is
/// PostgreSQL's leading underscore over the element's own `typname`.
pub(crate) fn array_typname(elem: crabka_pgtypes::ElemType) -> &'static str {
    use crabka_pgtypes::ElemType;
    match elem {
        ElemType::Bool => "_bool",
        ElemType::Json => "_json",
        ElemType::Xml => "_xml",
        ElemType::Int4 => "_int4",
        ElemType::Int8 => "_int8",
        ElemType::Text => "_text",
        ElemType::Float8 => "_float8",
        ElemType::Numeric => "_numeric",
        ElemType::Date => "_date",
        ElemType::Time => "_time",
        ElemType::Timestamp => "_timestamp",
        ElemType::Timestamptz => "_timestamptz",
        ElemType::Interval => "_interval",
        ElemType::Bytea => "_bytea",
        ElemType::Uuid => "_uuid",
        ElemType::Jsonb => "_jsonb",
        ElemType::JsonPath => "_jsonpath",
        ElemType::Int2 => "_int2",
        ElemType::Float4 => "_float4",
        ElemType::Regtype => "_regtype",
        ElemType::Varchar(_) => "_varchar",
        ElemType::Char(_) => "_bpchar",
        ElemType::Range(range) => match range.oid {
            crabka_pgtypes::oids::INT4RANGE => "_int4range",
            crabka_pgtypes::oids::NUMRANGE => "_numrange",
            crabka_pgtypes::oids::TSRANGE => "_tsrange",
            crabka_pgtypes::oids::TSTZRANGE => "_tstzrange",
            crabka_pgtypes::oids::DATERANGE => "_daterange",
            crabka_pgtypes::oids::INT8RANGE => "_int8range",
            _ => "_range",
        },
        ElemType::Multirange(multirange) => match multirange.oid {
            crabka_pgtypes::oids::INT4MULTIRANGE => "_int4multirange",
            crabka_pgtypes::oids::NUMMULTIRANGE => "_nummultirange",
            crabka_pgtypes::oids::TSMULTIRANGE => "_tsmultirange",
            crabka_pgtypes::oids::TSTZMULTIRANGE => "_tstzmultirange",
            crabka_pgtypes::oids::DATEMULTIRANGE => "_datemultirange",
            crabka_pgtypes::oids::INT8MULTIRANGE => "_int8multirange",
            _ => "_multirange",
        },
    }
}

/// The scalar rows plus one array row per supported element type (and `_json`,
/// the array of the `json` input alias). Array types are base types like their
/// elements (`typtype` 'b'), in category 'A', variable length, and they carry
/// the element's oid in `typelem`.
/// `pg_type.typname` for a built-in oid.
pub(crate) fn builtin_type_name(oid: u32) -> Option<&'static str> {
    builtin_type_rows()
        .iter()
        .find(|row| u32::try_from(row.oid) == Ok(oid))
        .map(|row| row.name)
}

pub(crate) fn builtin_type_rows() -> &'static [BuiltinTypeRow] {
    static ROWS: std::sync::LazyLock<Vec<BuiltinTypeRow>> = std::sync::LazyLock::new(|| {
        let mut rows = scalar_type_rows().to_vec();
        rows.extend([
            BuiltinTypeRow {
                oid: 1013,
                name: "_oidvector",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::OIDVECTOR as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: INT2VECTOR_ARRAY_OID,
                name: "_int2vector",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::INT2VECTOR as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: TIMETZ_ARRAY_OID,
                name: "_timetz",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::TIMETZ as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: crabka_pgtypes::oids::CHARARRAY as i32,
                name: "_char",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::CHAR as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: 2207,
                name: "_regprocedure",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::REGPROCEDURE as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: crabka_pgtypes::oids::REGNAMESPACEARRAY as i32,
                name: "_regnamespace",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::REGNAMESPACE as i32,
                array: 0,
            },
            // Neither snapshot type has an `ElemType`, so crabka can build no
            // array of either. The rows are here for the same reason the
            // `reg*` array rows below are: `typarray` has to point at a type
            // that exists, and `type_sanity` checks exactly that link.
            BuiltinTypeRow {
                oid: PG_SNAPSHOT_ARRAY_OID,
                name: "_pg_snapshot",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::PG_SNAPSHOT as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: TXID_SNAPSHOT_ARRAY_OID,
                name: "_txid_snapshot",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::TXID_SNAPSHOT as i32,
                array: 0,
            },
        ]);
        // The rest of the `reg*` family. crabka refuses to *build* an array of
        // any of them, but `pg_type` still has to describe the array type each
        // scalar's `typarray` points at — `type_sanity` checks exactly that
        // link, and a dangling one is a catalog error whether or not the type
        // is constructible.
        rows.extend(
            [
                (1008, "_regproc", crabka_pgtypes::oids::REGPROC),
                (2208, "_regoper", crabka_pgtypes::oids::REGOPER),
                (2209, "_regoperator", crabka_pgtypes::oids::REGOPERATOR),
                (2210, "_regclass", crabka_pgtypes::oids::REGCLASS),
                (3735, "_regconfig", crabka_pgtypes::oids::REGCONFIG),
                (3770, "_regdictionary", crabka_pgtypes::oids::REGDICTIONARY),
                (4097, "_regrole", crabka_pgtypes::oids::REGROLE),
                (4192, "_regcollation", crabka_pgtypes::oids::REGCOLLATION),
            ]
            .map(|(oid, name, elem)| BuiltinTypeRow {
                oid,
                name,
                len: -1,
                category: "A",
                elem: elem as i32,
                array: 0,
            }),
        );
        for (oid, name, array) in [
            (
                crabka_pgtypes::oids::INT4MULTIRANGE,
                "int4multirange",
                crabka_pgtypes::oids::INT4MULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::NUMMULTIRANGE,
                "nummultirange",
                crabka_pgtypes::oids::NUMMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::TSMULTIRANGE,
                "tsmultirange",
                crabka_pgtypes::oids::TSMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::TSTZMULTIRANGE,
                "tstzmultirange",
                crabka_pgtypes::oids::TSTZMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::DATEMULTIRANGE,
                "datemultirange",
                crabka_pgtypes::oids::DATEMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::INT8MULTIRANGE,
                "int8multirange",
                crabka_pgtypes::oids::INT8MULTIRANGEARRAY,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "R",
                elem: 0,
                array: array as i32,
            });
        }
        rows.push(BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSONARRAY as i32,
            name: "_json",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::JSON as i32,
            array: 0,
        });
        rows.push(BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSVECTORARRAY as i32,
            name: "_tsvector",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::TSVECTOR as i32,
            array: 0,
        });
        rows.push(BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSQUERYARRAY as i32,
            name: "_tsquery",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::TSQUERY as i32,
            array: 0,
        });
        // The network types' array rows exist so `pg_type.typarray` resolves
        // and a driver's typeinfo query finds them, exactly as PostgreSQL's do.
        // Building a value of one is a separate matter — `ElemType` has no
        // network variant, the same position the geometric types are in.
        for (oid, name, elem) in [
            (
                crabka_pgtypes::oids::INETARRAY,
                "_inet",
                crabka_pgtypes::oids::INET,
            ),
            (
                crabka_pgtypes::oids::CIDRARRAY,
                "_cidr",
                crabka_pgtypes::oids::CIDR,
            ),
            (
                crabka_pgtypes::oids::MACADDRARRAY,
                "_macaddr",
                crabka_pgtypes::oids::MACADDR,
            ),
            (
                crabka_pgtypes::oids::MACADDR8ARRAY,
                "_macaddr8",
                crabka_pgtypes::oids::MACADDR8,
            ),
            // `money` and the two bit types are in the same position: the array
            // row exists so `typarray` resolves, but `ElemType` has no variant
            // for them, so building a value of one is still 0A000.
            (
                crabka_pgtypes::oids::MONEYARRAY,
                "_money",
                crabka_pgtypes::oids::MONEY,
            ),
            (
                crabka_pgtypes::oids::BITARRAY,
                "_bit",
                crabka_pgtypes::oids::BIT,
            ),
            (
                crabka_pgtypes::oids::VARBITARRAY,
                "_varbit",
                crabka_pgtypes::oids::VARBIT,
            ),
            // The system identifier types are in that same position.
            (
                crabka_pgtypes::oids::OIDARRAY,
                "_oid",
                crabka_pgtypes::oids::OID,
            ),
            (
                crabka_pgtypes::oids::XIDARRAY,
                "_xid",
                crabka_pgtypes::oids::XID,
            ),
            (
                crabka_pgtypes::oids::XID8ARRAY,
                "_xid8",
                crabka_pgtypes::oids::XID8,
            ),
            (
                crabka_pgtypes::oids::CIDARRAY,
                "_cid",
                crabka_pgtypes::oids::CID,
            ),
            (
                crabka_pgtypes::oids::TIDARRAY,
                "_tid",
                crabka_pgtypes::oids::TID,
            ),
            (
                crabka_pgtypes::oids::PG_LSNARRAY,
                "_pg_lsn",
                crabka_pgtypes::oids::PG_LSN,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "A",
                elem: elem as i32,
                array: 0,
            });
        }
        for (oid, name, elem) in [
            (
                crabka_pgtypes::oids::INT4RANGEARRAY,
                "_int4range",
                crabka_pgtypes::oids::INT4RANGE,
            ),
            (
                crabka_pgtypes::oids::NUMRANGEARRAY,
                "_numrange",
                crabka_pgtypes::oids::NUMRANGE,
            ),
            (
                crabka_pgtypes::oids::TSRANGEARRAY,
                "_tsrange",
                crabka_pgtypes::oids::TSRANGE,
            ),
            (
                crabka_pgtypes::oids::TSTZRANGEARRAY,
                "_tstzrange",
                crabka_pgtypes::oids::TSTZRANGE,
            ),
            (
                crabka_pgtypes::oids::DATERANGEARRAY,
                "_daterange",
                crabka_pgtypes::oids::DATERANGE,
            ),
            (
                crabka_pgtypes::oids::INT8RANGEARRAY,
                "_int8range",
                crabka_pgtypes::oids::INT8RANGE,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "A",
                elem: elem as i32,
                array: 0,
            });
        }
        for (oid, name, elem) in [
            (
                crabka_pgtypes::oids::INT4MULTIRANGEARRAY,
                "_int4multirange",
                crabka_pgtypes::oids::INT4MULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::NUMMULTIRANGEARRAY,
                "_nummultirange",
                crabka_pgtypes::oids::NUMMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::TSMULTIRANGEARRAY,
                "_tsmultirange",
                crabka_pgtypes::oids::TSMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::TSTZMULTIRANGEARRAY,
                "_tstzmultirange",
                crabka_pgtypes::oids::TSTZMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::DATEMULTIRANGEARRAY,
                "_datemultirange",
                crabka_pgtypes::oids::DATEMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::INT8MULTIRANGEARRAY,
                "_int8multirange",
                crabka_pgtypes::oids::INT8MULTIRANGE,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "A",
                elem: elem as i32,
                array: 0,
            });
        }
        rows.extend(crabka_pgtypes::ElemType::ALL.map(|elem| BuiltinTypeRow {
            oid: i32::try_from(elem.array_oid()).expect("array oid fits in int4"),
            name: array_typname(elem),
            len: -1,
            category: "A",
            elem: i32::try_from(elem.oid()).expect("element oid fits in int4"),
            array: 0,
        }));
        rows
    });
    &ROWS
}

/// Whether `name` is an exact `pg_catalog.pg_type.typname`, excluding SQL
/// aliases such as `int` that parse as built-ins but are not catalog objects.
pub(crate) fn is_builtin_catalog_type_name(name: &str) -> bool {
    builtin_type_rows().iter().any(|ty| ty.name == name)
}

pub(crate) fn oid_i32(oid: u32) -> Result<i32, ExecError> {
    i32::try_from(oid).map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))
}

pub(crate) fn catalog_index_oid(index_id: u32) -> Result<i32, ExecError> {
    let oid = 50_000u32
        .checked_add(index_id)
        .ok_or_else(|| ExecError::Unsupported("index oid exceeds int4 range".into()))?;
    oid_i32(oid)
}

pub(crate) fn usize_i32(value: usize) -> Result<i32, ExecError> {
    i32::try_from(value)
        .map_err(|_| ExecError::Unsupported("catalog value exceeds int4 range".into()))
}

pub(crate) fn int(value: i32) -> Datum {
    Datum::Int4(value)
}

fn oid(value: i32) -> Datum {
    Datum::Oid(value as u32)
}

pub(crate) fn text(value: &str) -> Datum {
    Datum::Text(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_length_type_is_not_toastable() {
        let row = pg_type_row(
            PgTypeRow {
                oid: 1,
                name: "synthetic",
                namespace: PG_CATALOG_NAMESPACE_OID,
                len: 0,
                category: "P",
                typtype: "p",
                typrelid: 0,
                typelem: 0,
                typarray: 0,
                typbasetype: 0,
                typcollation: 0,
                domain_base: None,
                range_align: None,
            },
            &BTreeMap::new(),
        );
        assert!(row[23] == Datum::InternalChar(b'p'));
    }
}
