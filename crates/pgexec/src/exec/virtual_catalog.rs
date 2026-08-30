use super::*;

pub(crate) fn virtual_lookup_key(name: &crabka_pgcatalog::RelationName) -> String {
    if name.schema == crate::search_path::PG_CATALOG {
        name.name.clone()
    } else {
        format!("{}.{}", name.schema, name.name)
    }
}

pub(crate) fn is_virtual_relation(name: &crabka_pgcatalog::RelationName) -> bool {
    virtual_table(&virtual_lookup_key(name)).is_some()
}

pub(crate) fn virtual_relation_kind(name: &crabka_pgcatalog::RelationName) -> Option<&'static str> {
    let relation = virtual_table(&virtual_lookup_key(name))?;
    Some(match virtual_pg_class_properties(relation, 0).0 {
        "v" => "view",
        _ => "table",
    })
}

pub(crate) fn virtual_relation_table(
    name: &crabka_pgcatalog::RelationName,
) -> Option<crabka_pgcatalog::Table> {
    virtual_table(&virtual_lookup_key(name)).map(virtual_catalog_table)
}

pub(crate) fn is_system_catalog(name: &crabka_pgcatalog::RelationName) -> bool {
    virtual_relation_kind(name) == Some("table")
}

fn system_catalog_refusal(name: &crabka_pgcatalog::RelationName) -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42501",
        format!("permission denied: \"{}\" is a system catalog", name.name),
    ))
}

pub(crate) fn system_catalog_wrong_kind(
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    is_system_catalog(name).then(|| system_catalog_refusal(name))
}

pub(crate) fn virtual_table(name: &str) -> Option<&'static str> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "pg_namespace" => Some("pg_namespace"),
        "pg_class" => Some("pg_class"),
        "pg_attribute" => Some("pg_attribute"),
        "pg_type" => Some("pg_type"),
        "pg_ts_config" => Some("pg_ts_config"),
        "pg_ts_dict" => Some("pg_ts_dict"),
        "pg_range" => Some("pg_range"),
        "pg_index" => Some("pg_index"),
        "pg_cursors" => Some("pg_cursors"),
        "pg_settings" => Some("pg_settings"),
        "pg_prepared_xacts" => Some("pg_prepared_xacts"),
        "pg_prepared_statements" => Some("pg_prepared_statements"),
        "pg_roles" => Some("pg_roles"),
        "pg_user" => Some("pg_user"),
        "pg_statistic" => Some("pg_statistic"),
        "pg_stats" => Some("pg_stats"),
        "pg_stats_ext" => Some("pg_stats_ext"),
        _ => None,
    }
    .or_else(|| match name.strip_prefix("information_schema.")? {
        "schemata" => Some("information_schema.schemata"),
        "tables" => Some("information_schema.tables"),
        "columns" => Some("information_schema.columns"),
        "triggers" => Some("information_schema.triggers"),
        "triggered_update_columns" => Some("information_schema.triggered_update_columns"),
        _ => None,
    })
    .or_else(|| crate::catalog_rel::catalog_relation(name))
}

pub(crate) fn virtual_catalog_table(name: &str) -> Table {
    Table {
        id: virtual_relation_oid(name) as u32,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: crabka_pgcatalog::RelationName::new(
            virtual_relation_schema(name),
            virtual_relation_name(name),
        ),
        columns: virtual_catalog_columns(name),
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    }
}

/// Column definitions for synthesized catalog relations.
pub(crate) fn virtual_catalog_columns(name: &str) -> Vec<Column> {
    use ColumnType::{Bool, Int2, Int4, Int8, Text, Timestamptz};
    match name {
        "pg_namespace" => cols(&[
            ("oid", Int4),
            ("nspname", Text),
            // `\dn` reads `nspowner` through `pg_get_userbyid`.
            ("nspowner", Int4),
            ("nspacl", ColumnType::Array(crabka_pgtypes::ElemType::Text)),
        ]),
        // PostgreSQL 18.4's column set, in catalog order: `psql`'s `\d` reads
        // relpersistence/relreplident/relchecks/relhasrules/relhastriggers/
        // relrowsecurity/relispartition/reloftype/reltablespace/relam by name,
        // and ORMs positionally `SELECT *`.
        "pg_class" => pg_class_columns(),
        "pg_attribute" => pg_attribute_columns(),
        "pg_type" => cols(&[
            ("oid", ColumnType::Oid),
            ("typname", Text),
            ("typnamespace", ColumnType::Oid),
            ("typowner", ColumnType::Oid),
            ("typlen", Int2),
            ("typbyval", Bool),
            ("typtype", ColumnType::InternalChar),
            ("typcategory", ColumnType::InternalChar),
            ("typispreferred", Bool),
            ("typisdefined", Bool),
            ("typdelim", ColumnType::InternalChar),
            ("typrelid", ColumnType::Oid),
            ("typsubscript", ColumnType::Regproc),
            ("typelem", ColumnType::Oid),
            ("typarray", ColumnType::Oid),
            ("typinput", ColumnType::Regproc),
            ("typoutput", ColumnType::Regproc),
            ("typreceive", ColumnType::Regproc),
            ("typsend", ColumnType::Regproc),
            ("typmodin", ColumnType::Regproc),
            ("typmodout", ColumnType::Regproc),
            ("typanalyze", ColumnType::Regproc),
            ("typalign", ColumnType::InternalChar),
            ("typstorage", ColumnType::InternalChar),
            ("typnotnull", Bool),
            ("typbasetype", ColumnType::Oid),
            ("typtypmod", Int4),
            ("typndims", Int4),
            ("typcollation", ColumnType::Oid),
            ("typdefaultbin", Text),
            ("typdefault", Text),
            ("typacl", ColumnType::Array(crabka_pgtypes::ElemType::Text)),
        ]),
        "pg_ts_config" => cols(&[
            ("oid", Int4),
            ("cfgname", Text),
            ("cfgnamespace", Int4),
            ("cfgowner", Int4),
            ("cfgparser", Int4),
        ]),
        "pg_ts_dict" => cols(&[
            ("oid", Int4),
            ("dictname", Text),
            ("dictnamespace", Int4),
            ("dictowner", Int4),
            ("dicttemplate", Int4),
            ("dictinitoption", Text),
        ]),
        // PostgreSQL 18 column set, in catalog order.
        "pg_range" => cols(&[
            ("rngtypid", ColumnType::Oid),
            ("rngsubtype", ColumnType::Oid),
            ("rngmultitypid", ColumnType::Oid),
            ("rngcollation", ColumnType::Oid),
            ("rngsubopc", ColumnType::Oid),
            ("rngcanonical", ColumnType::Regproc),
            ("rngsubdiff", ColumnType::Regproc),
        ]),
        // PostgreSQL 18.4's column set, in catalog order. `\d <table>` reads
        // indisclustered/indisvalid/indisreplident by name and joins
        // `indexrelid`/`indrelid` against `pg_class`.
        "pg_index" => cols(&[
            ("indexrelid", Int4),
            ("indrelid", Int4),
            ("indnatts", Int2),
            ("indnkeyatts", Int2),
            ("indisunique", Bool),
            ("indnullsnotdistinct", Bool),
            ("indisprimary", Bool),
            ("indisexclusion", Bool),
            ("indimmediate", Bool),
            ("indisclustered", Bool),
            ("indisvalid", Bool),
            ("indcheckxmin", Bool),
            ("indisready", Bool),
            ("indislive", Bool),
            ("indisreplident", Bool),
            // PostgreSQL's `int2vector` is a zero-based, space-rendered vector.
            // `OidVector` is crabka's existing representation with those same
            // catalog-facing semantics, including `indkey[0]` subscripting.
            ("indkey", ColumnType::OidVector),
            ("indcollation", ColumnType::OidVector),
            ("indclass", ColumnType::OidVector),
            ("indoption", ColumnType::Int2Vector),
            ("indexprs", Text),
            ("indpred", Text),
        ]),
        "pg_prepared_statements" => cols(&[
            ("name", Text),
            ("statement", Text),
            ("prepare_time", Timestamptz),
            ("parameter_types", Text),
            ("result_types", Text),
            ("from_sql", Bool),
            ("generic_plans", Int8),
            ("custom_plans", Int8),
        ]),
        "pg_cursors" => cols(&[
            ("name", Text),
            ("statement", Text),
            ("is_holdable", Bool),
            ("is_binary", Bool),
            ("is_scrollable", Bool),
            ("creation_time", Timestamptz),
        ]),
        "pg_prepared_xacts" => cols(&[
            ("transaction", ColumnType::Xid),
            ("gid", Text),
            ("prepared", Timestamptz),
            ("owner", Text),
            ("database", Text),
        ]),
        "pg_settings" => cols(&[
            ("name", Text),
            ("setting", Text),
            ("unit", Text),
            ("category", Text),
            ("short_desc", Text),
            ("context", Text),
            ("vartype", Text),
            ("source", Text),
            ("min_val", Text),
            ("max_val", Text),
            ("enumvals", Text),
            ("boot_val", Text),
            ("reset_val", Text),
            ("pending_restart", Bool),
        ]),
        // PostgreSQL 18.4's column set, in catalog order — `\du` projects
        // rolinherit/rolconnlimit/rolvaliduntil positionally after the flags.
        "pg_roles" => cols(&[
            ("rolname", Text),
            ("rolsuper", Bool),
            ("rolinherit", Bool),
            ("rolcreaterole", Bool),
            ("rolcreatedb", Bool),
            ("rolcanlogin", Bool),
            ("rolreplication", Bool),
            ("rolconnlimit", Int4),
            ("rolpassword", Text),
            ("rolvaliduntil", Timestamptz),
            ("rolbypassrls", Bool),
            (
                "rolconfig",
                ColumnType::Array(crabka_pgtypes::ElemType::Text),
            ),
            ("oid", Int4),
        ]),
        "pg_user" => cols(&[("usename", Text), ("usesuper", Bool), ("usecreatedb", Bool)]),
        "pg_statistic" => cols(&[
            ("starelid", Int4),
            ("staattnum", Int2),
            ("stainherit", Bool),
            ("stanullfrac", ColumnType::Float4),
            ("stawidth", Int4),
            ("stadistinct", ColumnType::Float4),
            ("stakind1", Int2),
            ("stakind2", Int2),
            ("stakind3", Int2),
            ("stakind4", Int2),
            ("stakind5", Int2),
            ("staop1", Int4),
            ("staop2", Int4),
            ("staop3", Int4),
            ("staop4", Int4),
            ("staop5", Int4),
            ("stacoll1", Int4),
            ("stacoll2", Int4),
            ("stacoll3", Int4),
            ("stacoll4", Int4),
            ("stacoll5", Int4),
            (
                "stanumbers1",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            (
                "stanumbers2",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            (
                "stanumbers3",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            (
                "stanumbers4",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            (
                "stanumbers5",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            // PostgreSQL uses anyarray here. The executor only needs these
            // values as text (including pg_dump's `::text` projection), so the
            // durable canonical array text is the narrowest truthful surface.
            ("stavalues1", Text),
            ("stavalues2", Text),
            ("stavalues3", Text),
            ("stavalues4", Text),
            ("stavalues5", Text),
        ]),
        "pg_stats" => cols(&[
            ("schemaname", Text),
            ("tablename", Text),
            ("attname", Text),
            ("inherited", Bool),
            ("null_frac", ColumnType::Float4),
            ("avg_width", Int4),
            ("n_distinct", ColumnType::Float4),
            ("most_common_vals", Text),
            (
                "most_common_freqs",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            ("histogram_bounds", Text),
            ("correlation", ColumnType::Float4),
            ("most_common_elems", Text),
            (
                "most_common_elem_freqs",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            (
                "elem_count_histogram",
                ColumnType::Array(crabka_pgtypes::ElemType::Float4),
            ),
            ("range_length_histogram", Text),
            ("range_empty_frac", ColumnType::Float4),
            ("range_bounds_histogram", Text),
        ]),
        "pg_stats_ext" => cols(&[
            ("schemaname", Text),
            ("tablename", Text),
            ("statistics_schemaname", Text),
            ("statistics_name", Text),
            ("statistics_owner", Text),
            (
                "attnames",
                ColumnType::Array(crabka_pgtypes::ElemType::Text),
            ),
            ("exprs", ColumnType::Array(crabka_pgtypes::ElemType::Text)),
            ("kinds", ColumnType::Array(crabka_pgtypes::ElemType::Text)),
            ("inherited", Bool),
            ("n_distinct", Text),
            ("dependencies", Text),
            ("most_common_vals", Text),
            ("most_common_val_nulls", Text),
            (
                "most_common_freqs",
                ColumnType::Array(crabka_pgtypes::ElemType::Float8),
            ),
            (
                "most_common_base_freqs",
                ColumnType::Array(crabka_pgtypes::ElemType::Float8),
            ),
        ]),
        // The full standard projection, in PostgreSQL 18.4's column order. The
        // three `default_character_set_*` columns and `sql_path` are NULL in
        // PostgreSQL too — the standard defines them, PostgreSQL fills none.
        "information_schema.schemata" => cols(&[
            ("catalog_name", Text),
            ("schema_name", Text),
            ("schema_owner", Text),
            ("default_character_set_catalog", Text),
            ("default_character_set_schema", Text),
            ("default_character_set_name", Text),
            ("sql_path", Text),
        ]),
        // Not PostgreSQL's full 12-column list: the five NULL columns between
        // `table_type` and `is_insertable_into`, and the two after it, are not
        // synthesized here, so `is_insertable_into` sits at ordinal 5 rather
        // than 10. Every consumer names the column, and none of the absent ones
        // carries a value for an untyped relation.
        "information_schema.tables" => cols(&[
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("table_type", Text),
            ("is_insertable_into", Text),
        ]),
        // As with `information_schema.tables`, a subset of PostgreSQL's list —
        // `is_updatable` is its 44th column and is appended here rather than
        // the 37 columns before it being synthesized.
        "information_schema.columns" => cols(&[
            ("table_schema", Text),
            ("table_name", Text),
            ("column_name", Text),
            ("ordinal_position", Int4),
            ("data_type", Text),
            ("is_nullable", Text),
            ("column_default", Text),
            ("is_updatable", Text),
        ]),
        "information_schema.triggers" => cols(&[
            ("trigger_catalog", Text),
            ("trigger_schema", Text),
            ("trigger_name", Text),
            ("event_manipulation", Text),
            ("event_object_catalog", Text),
            ("event_object_schema", Text),
            ("event_object_table", Text),
            ("action_order", Int4),
            ("action_condition", Text),
            ("action_statement", Text),
            ("action_orientation", Text),
            ("action_timing", Text),
            ("action_reference_old_table", Text),
            ("action_reference_new_table", Text),
            ("action_reference_old_row", Text),
            ("action_reference_new_row", Text),
            ("created", Timestamptz),
        ]),
        "information_schema.triggered_update_columns" => cols(&[
            ("trigger_catalog", Text),
            ("trigger_schema", Text),
            ("trigger_name", Text),
            ("event_object_catalog", Text),
            ("event_object_schema", Text),
            ("event_object_table", Text),
            ("event_object_column", Text),
        ]),
        _ => crate::catalog_rel::columns(name),
    }
}
