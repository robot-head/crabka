//! F-2: the `pg_catalog`/`information_schema` breadth `psql`'s `\d` family and
//! ORM preambles depend on.
//!
//! These tests exercise that breadth through the SQL session, the way a client
//! reaches it.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
    engine
        .connect()
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .into_iter()
        .next()
        .expect("one result")
}

/// Every row of a single-column result, in order.
async fn column(engine: &SqlEngine, sql: &str) -> Vec<Option<String>> {
    grid(engine, sql)
        .await
        .into_iter()
        .map(|row| row.into_iter().next().expect("one column"))
        .collect()
}

/// The whole result as text, so a test can compare an entire expected table.
async fn grid(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    result_grid(run(engine, sql).await)
}

fn result_grid(result: QueryResult) -> Vec<Vec<Option<String>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| cell.as_ref().map(text_of))
                    .collect::<Vec<_>>()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn text_of(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("valid text cell")
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

async fn fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE shop (id int4 PRIMARY KEY, code text NOT NULL, price numeric(9,2))",
    )
    .await;
    run(&engine, "CREATE INDEX shop_code_idx ON shop (code)").await;
    run(&engine, "CREATE VIEW shop_v AS SELECT id, code FROM shop").await;
    engine
}

/// Every relation this wave names must resolve. Where crabka has no such object
/// kind yet, the answer is zero rows, and never
/// `relation ... does not exist`.
#[tokio::test]
async fn every_named_catalog_relation_resolves() {
    let engine = SqlEngine::new();
    for relation in [
        "pg_catalog.pg_class",
        "pg_catalog.pg_attribute",
        "pg_catalog.pg_attrdef",
        "pg_catalog.pg_namespace",
        "pg_catalog.pg_type",
        "pg_catalog.pg_index",
        "pg_catalog.pg_constraint",
        "pg_catalog.pg_conversion",
        "pg_catalog.pg_proc",
        "pg_catalog.pg_description",
        "pg_catalog.pg_roles",
        "pg_catalog.pg_authid",
        "pg_catalog.pg_cast",
        "pg_catalog.pg_database",
        "pg_catalog.pg_tablespace",
        "pg_catalog.pg_aggregate",
        "pg_catalog.pg_am",
        "pg_catalog.pg_inherits",
        "pg_catalog.pg_language",
        "pg_catalog.pg_extension",
        "pg_catalog.pg_foreign_data_wrapper",
        "pg_catalog.pg_foreign_server",
        "pg_catalog.pg_foreign_table",
        "pg_catalog.pg_depend",
        "pg_catalog.pg_rewrite",
        "pg_catalog.pg_trigger",
        "pg_catalog.pg_user_mapping",
        "pg_catalog.pg_sequence",
        "pg_catalog.pg_collation",
        "pg_catalog.pg_enum",
        "pg_catalog.pg_range",
        "pg_catalog.pg_shdepend",
        "pg_catalog.pg_settings",
        "pg_catalog.pg_stat_activity",
        "pg_catalog.pg_locks",
        "pg_catalog.pg_replication_slots",
        "pg_catalog.pg_policy",
        "pg_catalog.pg_publication",
        "pg_catalog.pg_statistic_ext",
        "pg_catalog.pg_tables",
        "pg_catalog.pg_views",
        "pg_catalog.pg_indexes",
        "information_schema.table_constraints",
        "information_schema.key_column_usage",
        "information_schema.constraint_column_usage",
        "information_schema.referential_constraints",
        "information_schema.views",
        "information_schema.routines",
        "information_schema.parameters",
        "information_schema.sequences",
        "information_schema.table_privileges",
        "information_schema.column_privileges",
        "information_schema.enabled_roles",
        "information_schema.applicable_roles",
    ] {
        let sql = format!("SELECT count(*) FROM {relation}");
        let result = engine
            .connect()
            .simple_query(&sql)
            .await
            .unwrap_or_else(|error| panic!("{relation} does not resolve: {error:?}"));
        assert2::assert!(result.len() == 1, "{relation}");
    }
}

#[tokio::test]
async fn pg_foreign_catalogs_list_the_registered_objects() {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE FUNCTION fdw_handler() RETURNS fdw_handler LANGUAGE c AS 'regress', 'test_fdw_handler'",
    )
    .await;
    run(
        &engine,
        "CREATE FOREIGN DATA WRAPPER fdw HANDLER fdw_handler VALIDATOR postgresql_fdw_validator OPTIONS (kind 'test')",
    )
    .await;
    run(
        &engine,
        "CREATE SERVER server TYPE 'test' VERSION '1' FOREIGN DATA WRAPPER fdw OPTIONS (host 'local')",
    )
    .await;
    run(
        &engine,
        "CREATE USER MAPPING FOR CURRENT_USER SERVER server",
    )
    .await;
    run(
        &engine,
        "CREATE FOREIGN TABLE remote (value text) SERVER server OPTIONS (topic 'events')",
    )
    .await;

    assert!(
        grid(
            &engine,
            "SELECT fdwname, fdwhandler::text, fdwvalidator::text, fdwoptions FROM pg_foreign_data_wrapper",
        )
        .await
            == vec![some(&[
                "fdw",
                "fdw_handler",
                "postgresql_fdw_validator",
                "{kind=test}"
            ])]
    );
    assert!(
        grid(
            &engine,
            "SELECT array_to_string(ARRAY(SELECT option_name FROM pg_options_to_table(fdwoptions)), ',') \
             FROM pg_catalog.pg_foreign_data_wrapper fdw \
             LEFT JOIN pg_catalog.pg_description d \
               ON d.classoid = fdw.tableoid AND d.objoid = fdw.oid AND d.objsubid = 0",
        )
        .await
            == vec![some(&["kind"])]
    );
    assert!(
        grid(
            &engine,
            "SELECT srvname, srvtype, srvversion, srvoptions FROM pg_foreign_server",
        )
        .await
            == vec![some(&["server", "test", "1", "{host=local}"])]
    );
    assert!(
        grid(
            &engine,
            "SELECT umuser = 0, umserver > 0 FROM pg_user_mapping",
        )
        .await
            == vec![some(&["f", "t"])]
    );
    assert!(
        grid(&engine, "SELECT srvname, usename FROM pg_user_mappings").await
            == vec![some(&["server", "postgres"])]
    );
    assert!(
        grid(
            &engine,
            "SELECT ftserver > 0, ftoptions FROM pg_foreign_table",
        )
        .await
            == vec![some(&["t", "{topic=events}"])]
    );
}

#[tokio::test]
async fn pg_locks_renders_live_relation_and_advisory_holds() {
    let engine = SqlEngine::new();
    let mut session = engine.connect_with_pid(4242);
    session
        .simple_query("CREATE TABLE held (id int4)")
        .await
        .expect("create table");
    let relation = result_grid(
        session
            .simple_query("SELECT oid FROM pg_class WHERE relname = 'held'")
            .await
            .expect("table oid")
            .into_iter()
            .next()
            .expect("one result"),
    )[0][0]
        .clone()
        .expect("relation oid");
    session
        .simple_query(
            "BEGIN; LOCK TABLE held IN SHARE MODE; SELECT pg_advisory_lock(1234567890123)",
        )
        .await
        .expect("take locks");

    let rows = result_grid(
        session
            .simple_query(
                "SELECT locktype, database, relation, classid, objid, objsubid, \
                        virtualtransaction, pid, mode, granted, fastpath \
                 FROM pg_locks WHERE pid = 4242 ORDER BY locktype",
            )
            .await
            .expect("list locks")
            .into_iter()
            .next()
            .expect("one result"),
    );

    assert!(
        rows == vec![
            vec![
                Some("advisory".into()),
                Some("5".into()),
                None,
                Some("287".into()),
                Some("1912276171".into()),
                Some("1".into()),
                Some("1/0".into()),
                Some("4242".into()),
                Some("ExclusiveLock".into()),
                Some("t".into()),
                Some("f".into()),
            ],
            vec![
                Some("relation".into()),
                Some("5".into()),
                Some(relation),
                None,
                None,
                None,
                Some("1/0".into()),
                Some("4242".into()),
                Some("ShareLock".into()),
                Some("t".into()),
                Some("f".into()),
            ],
        ],
        "pg_locks rows: {rows:?}"
    );
}

#[tokio::test]
async fn pg_locks_renders_a_live_tuple_lock() {
    let engine = SqlEngine::new();
    let mut holder = engine.connect_with_pid(4242);
    holder
        .simple_query("CREATE TABLE tuple_held (id int4); INSERT INTO tuple_held VALUES (7)")
        .await
        .expect("create tuple");
    let relation = result_grid(
        holder
            .simple_query("SELECT oid FROM pg_class WHERE relname = 'tuple_held'")
            .await
            .expect("table oid")
            .into_iter()
            .next()
            .expect("one result"),
    )[0][0]
        .clone()
        .expect("relation oid");
    holder
        .simple_query("BEGIN; SELECT * FROM tuple_held FOR UPDATE")
        .await
        .expect("lock tuple");
    let mut observer = engine.connect_with_pid(4343);
    let rows = result_grid(
        observer
            .simple_query(
                "SELECT locktype, database, relation, page, tuple, pid, mode, granted \
                 FROM pg_locks WHERE pid = 4242 AND locktype = 'tuple'",
            )
            .await
            .expect("list tuple lock")
            .into_iter()
            .next()
            .expect("one result"),
    );

    assert!(
        rows == vec![some(&[
            "tuple",
            "5",
            &relation,
            "0",
            "1",
            "4242",
            "For Update",
            "t",
        ])],
        "pg_locks rows: {rows:?}"
    );
}

#[tokio::test]
async fn pg_prepared_xacts_has_postgresqls_empty_2pc_shape() {
    let engine = SqlEngine::new();
    let QueryResult::Rows { fields, rows, .. } = run(
        &engine,
        "SELECT transaction, gid, prepared, owner, database FROM pg_prepared_xacts",
    )
    .await
    else {
        panic!("pg_prepared_xacts query should return rows");
    };
    assert!(rows.is_empty());
    assert!(
        fields
            .iter()
            .map(|field| (field.name.as_str(), field.type_oid))
            .collect::<Vec<_>>()
            == vec![
                ("transaction", 28),
                ("gid", 25),
                ("prepared", 1184),
                ("owner", 25),
                ("database", 25),
            ]
    );
    assert!(
        grid(
            &engine,
            "SELECT relkind, relfilenode, oid FROM pg_class WHERE relname = 'pg_prepared_xacts'",
        )
        .await
            == vec![some(&["v", "0", "100004"])]
    );
}

#[tokio::test]
async fn pg_am_uses_postgresql_handlers_and_oid_regproc_columns() {
    let engine = SqlEngine::new();
    let sql = "SELECT oid, amname, amhandler, amtype FROM pg_am ORDER BY oid";
    assert!(
        grid(&engine, sql).await
            == vec![
                some(&["2", "heap", "heap_tableam_handler", "t"]),
                some(&["403", "btree", "bthandler", "i"]),
                some(&["405", "hash", "hashhandler", "i"]),
                some(&["783", "gist", "gisthandler", "i"]),
                some(&["2742", "gin", "ginhandler", "i"]),
                some(&["3580", "brin", "brinhandler", "i"]),
                some(&["4000", "spgist", "spghandler", "i"]),
            ]
    );
    let QueryResult::Rows { fields, .. } = run(&engine, sql).await else {
        panic!("pg_am query should return rows");
    };
    assert!(
        fields
            .iter()
            .map(|field| field.type_oid)
            .collect::<Vec<_>>()
            == vec![26, 25, 24, 18]
    );
}

#[tokio::test]
async fn pg_shdepend_has_postgresqls_empty_bootstrap_shape() {
    let engine = SqlEngine::new();
    assert!(
        grid(
            &engine,
            "SELECT oid FROM pg_class WHERE relname = 'pg_shdepend'",
        )
        .await
            == vec![some(&["1214"])]
    );
    let QueryResult::Rows { fields, rows, .. } = run(
        &engine,
        "SELECT dbid, classid, objid, objsubid, refclassid, refobjid, deptype FROM pg_shdepend",
    )
    .await
    else {
        panic!("pg_shdepend query should return rows");
    };
    assert!(rows.is_empty());
    assert!(
        fields
            .iter()
            .map(|field| field.type_oid)
            .collect::<Vec<_>>()
            == vec![26, 26, 26, 23, 26, 26, 18]
    );
}

/// `PostgreSQL`'s own `sanity_check.sql` requires every low-oid system catalog
/// with an `oid` column to have an immediate, unique one-column oid index.
#[tokio::test]
async fn system_catalog_oid_indexes_pass_upstream_sanity_check() {
    let engine = SqlEngine::new();
    let missing = grid(
        &engine,
        "SELECT relname, nspname
         FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = relnamespace JOIN pg_attribute a ON (attrelid = c.oid AND attname = 'oid')
         WHERE relkind = 'r' and c.oid < 16384
             AND ((nspname ~ '^pg_') IS NOT FALSE)
             AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE indrelid = c.oid
                             AND indkey[0] = a.attnum AND indnatts = 1
                             AND indisunique AND indimmediate)",
    )
    .await;
    assert2::assert!(missing.is_empty(), "missing oid indexes: {missing:?}");
}

#[tokio::test]
async fn system_catalog_oid_indexes_keep_pg18_catalog_identity_and_links() {
    let engine = SqlEngine::new();
    let indexes = grid(
        &engine,
        "SELECT t.relname, x.oid, x.relname
         FROM pg_class t
         JOIN pg_attribute ta ON ta.attrelid = t.oid AND ta.attname = 'oid'
         JOIN pg_index i ON i.indrelid = t.oid AND i.indkey[0] = ta.attnum
         JOIN pg_class x ON x.oid = i.indexrelid
         JOIN pg_attribute xa ON xa.attrelid = x.oid AND xa.attnum = 1
         WHERE t.relnamespace = 11 AND t.relkind = 'r' AND i.indnatts = 1
               AND t.relhasindex AND x.relnamespace = 11 AND x.relkind = 'i'
               AND x.relam = 403 AND x.relnatts = 1
               AND (x.relfilenode = 0 OR x.relfilenode = x.oid)
               AND i.indnkeyatts = 1 AND i.indisunique AND i.indisprimary
               AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive
               AND i.indkey[1] IS NULL AND xa.attname = 'oid'
               AND xa.attnum = 1 AND xa.atttypid = ta.atttypid
         ORDER BY t.relname",
    )
    .await;
    assert2::assert!(
        indexes
            == vec![
                some(&["pg_am", "2652", "pg_am_oid_index"]),
                some(&["pg_amop", "2756", "pg_amop_oid_index"]),
                some(&["pg_amproc", "2757", "pg_amproc_oid_index"]),
                some(&["pg_attrdef", "2657", "pg_attrdef_oid_index"]),
                some(&["pg_authid", "2677", "pg_authid_oid_index"]),
                some(&["pg_cast", "2660", "pg_cast_oid_index"]),
                some(&["pg_class", "2662", "pg_class_oid_index"]),
                some(&["pg_collation", "3085", "pg_collation_oid_index"]),
                some(&["pg_constraint", "2667", "pg_constraint_oid_index"]),
                some(&["pg_conversion", "2670", "pg_conversion_oid_index"]),
                some(&["pg_database", "2672", "pg_database_oid_index"]),
                some(&["pg_enum", "3502", "pg_enum_oid_index"]),
                some(&["pg_event_trigger", "3468", "pg_event_trigger_oid_index"]),
                some(&["pg_extension", "3080", "pg_extension_oid_index"]),
                some(&[
                    "pg_foreign_data_wrapper",
                    "112",
                    "pg_foreign_data_wrapper_oid_index"
                ]),
                some(&["pg_foreign_server", "113", "pg_foreign_server_oid_index"]),
                some(&["pg_language", "2682", "pg_language_oid_index"]),
                some(&[
                    "pg_largeobject_metadata",
                    "2996",
                    "pg_largeobject_metadata_oid_index"
                ]),
                some(&["pg_namespace", "2685", "pg_namespace_oid_index"]),
                some(&["pg_opclass", "2687", "pg_opclass_oid_index"]),
                some(&["pg_operator", "2688", "pg_operator_oid_index"]),
                some(&["pg_opfamily", "2755", "pg_opfamily_oid_index"]),
                some(&["pg_policy", "3257", "pg_policy_oid_index"]),
                some(&["pg_proc", "2690", "pg_proc_oid_index"]),
                some(&["pg_publication", "6110", "pg_publication_oid_index"]),
                some(&[
                    "pg_publication_namespace",
                    "6238",
                    "pg_publication_namespace_oid_index"
                ]),
                some(&["pg_publication_rel", "6112", "pg_publication_rel_oid_index"]),
                some(&["pg_rewrite", "2692", "pg_rewrite_oid_index"]),
                some(&["pg_statistic_ext", "3380", "pg_statistic_ext_oid_index"]),
                some(&["pg_tablespace", "2697", "pg_tablespace_oid_index"]),
                some(&["pg_trigger", "2702", "pg_trigger_oid_index"]),
                some(&["pg_ts_config", "3712", "pg_ts_config_oid_index"]),
                some(&["pg_ts_dict", "3605", "pg_ts_dict_oid_index"]),
                some(&["pg_type", "2703", "pg_type_oid_index"]),
                some(&["pg_user_mapping", "174", "pg_user_mapping_oid_index"]),
            ]
    );
}

#[tokio::test]
async fn pg_cast_exposes_postgresql_builtin_casts() {
    let engine = SqlEngine::new();
    let listed = grid(
        &engine,
        "SELECT oid, castsource, casttarget, castfunc, castcontext, castmethod \
         FROM pg_catalog.pg_cast WHERE castsource = 23 AND casttarget = 26",
    )
    .await;
    assert2::assert!(listed == vec![some(&["10039", "23", "26", "0", "i", "b"])]);
}

#[tokio::test]
async fn pg_aggregate_exposes_postgresql_builtin_aggregates() {
    let engine = SqlEngine::new();
    let listed = grid(
        &engine,
        "SELECT aggfnoid, aggkind, aggnumdirectargs, aggtransfn, aggfinalfn, \
                aggcombinefn, aggserialfn, aggdeserialfn, aggtranstype, \
                aggtransspace, agginitval \
         FROM pg_catalog.pg_aggregate WHERE aggfnoid = 2100",
    )
    .await;
    assert2::assert!(
        listed
            == vec![vec![
                Some("2100".into()),
                Some("n".into()),
                Some("0".into()),
                Some("int8_avg_accum".into()),
                Some("numeric_poly_avg".into()),
                Some("int8_avg_combine".into()),
                Some("int8_avg_serialize".into()),
                Some("int8_avg_deserialize".into()),
                Some("internal".into()),
                Some("48".into()),
                None,
            ]]
    );
}

#[tokio::test]
async fn pg_conversion_exposes_postgresql_builtin_conversions() {
    let engine = SqlEngine::new();
    let listed = grid(
        &engine,
        "SELECT oid, conname, connamespace, conowner, conforencoding, \
                contoencoding, conproc, condefault \
         FROM pg_catalog.pg_conversion WHERE oid = 4402",
    )
    .await;
    assert2::assert!(
        listed
            == vec![some(&[
                "4402",
                "koi8_r_to_mic",
                "11",
                "10",
                "22",
                "7",
                "4302",
                "t"
            ])]
    );
}

#[tokio::test]
async fn pg_proc_support_oid_join_stays_indexable() {
    let engine = SqlEngine::new();
    let count = column(
        &engine,
        "SELECT count(*) FROM pg_proc p1, pg_proc p2 WHERE p2.oid = p1.prosupport",
    )
    .await;
    assert2::assert!(count == some(&["53"]));
}

#[tokio::test]
async fn pg_proc_signature_self_join_stays_indexable() {
    let engine = SqlEngine::new();
    let duplicates = grid(
        &engine,
        "SELECT p1.oid, p1.proname, p2.oid, p2.proname \
         FROM pg_proc AS p1, pg_proc AS p2 \
         WHERE p1.oid != p2.oid AND \
               p1.proname = p2.proname AND \
               p1.pronargs = p2.pronargs AND \
               p1.proargtypes = p2.proargtypes",
    )
    .await;
    assert2::assert!(duplicates.is_empty());
}

/// A bare `pg_catalog.`-less name resolves to the same relation as the
/// qualified one, because clients write both.
#[tokio::test]
async fn catalog_relations_resolve_with_and_without_the_schema_qualifier() {
    let engine = fixture().await;
    for relation in ["pg_am", "pg_constraint", "pg_tables", "pg_description"] {
        let bare = column(&engine, &format!("SELECT count(*) FROM {relation}")).await;
        let qualified = column(
            &engine,
            &format!("SELECT count(*) FROM pg_catalog.{relation}"),
        )
        .await;
        assert2::assert!(bare == qualified, "{relation}");
    }
}

/// `\dt`/`\dv`/`\di`/`\ds` differ only in the `relkind` they filter on, so one
/// `pg_class` listing has to describe all four relation kinds.
#[tokio::test]
async fn pg_class_describes_every_relation_kind() {
    let engine = fixture().await;
    let listed = grid(
        &engine,
        "SELECT relname, relkind, relnatts, relhasindex FROM pg_catalog.pg_class \
         WHERE relname IN ('shop', 'shop_v', 'shop_code_idx', 'shop_pkey') \
         ORDER BY relname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["shop", "r", "3", "t"]),
                some(&["shop_code_idx", "i", "1", "f"]),
                some(&["shop_pkey", "i", "1", "f"]),
                some(&["shop_v", "v", "2", "f"]),
            ]
    );
}

#[tokio::test]
async fn pg_class_reports_postgresql_relkind_and_storage_semantics() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE TABLE shop_parts (id int4) PARTITION BY RANGE (id)",
    )
    .await;
    run(&engine, "CREATE FOREIGN DATA WRAPPER shop_fdw").await;
    run(
        &engine,
        "CREATE SERVER shop_server FOREIGN DATA WRAPPER shop_fdw",
    )
    .await;
    run(
        &engine,
        "CREATE FOREIGN TABLE shop_remote (id int4) SERVER shop_server",
    )
    .await;
    run(&engine, "CREATE SEQUENCE shop_seq").await;

    let listed = grid(
        &engine,
        "SELECT n.nspname, c.relname, c.relkind, c.relam, c.oid > 0, \
                c.relfilenode = c.oid, c.relfilenode = 0 \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE (n.nspname = 'public' AND \
                c.relname IN ('shop', 'shop_v', 'shop_parts', 'shop_remote', \
                              'shop_seq')) OR \
               (n.nspname = 'pg_catalog' AND \
                c.relname IN ('pg_class', 'pg_partitioned_table', \
                              'pg_prepared_statements', 'pg_roles')) OR \
               (n.nspname = 'information_schema' AND c.relname = 'tables') \
         ORDER BY n.nspname, c.relname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["information_schema", "tables", "v", "0", "t", "f", "t"]),
                some(&["pg_catalog", "pg_class", "r", "2", "t", "f", "t"]),
                some(&[
                    "pg_catalog",
                    "pg_partitioned_table",
                    "r",
                    "2",
                    "t",
                    "t",
                    "f"
                ]),
                some(&[
                    "pg_catalog",
                    "pg_prepared_statements",
                    "v",
                    "0",
                    "t",
                    "f",
                    "t"
                ]),
                some(&["pg_catalog", "pg_roles", "v", "0", "t", "f", "t"]),
                some(&["public", "shop", "r", "2", "t", "t", "f"]),
                some(&["public", "shop_parts", "p", "0", "t", "f", "t"]),
                some(&["public", "shop_remote", "f", "0", "t", "f", "t"]),
                some(&["public", "shop_seq", "S", "0", "t", "t", "f"]),
                some(&["public", "shop_v", "v", "0", "t", "f", "t"]),
            ]
    );
}

#[tokio::test]
async fn pg_class_distinguishes_partitioned_and_ordinary_indexes() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE TABLE shop_parts (id int4) PARTITION BY RANGE (id)",
    )
    .await;
    run(&engine, "CREATE INDEX shop_parts_idx ON shop_parts (id)").await;

    let listed = grid(
        &engine,
        "SELECT relname, relkind, relam, relfilenode = oid, relfilenode = 0 \
         FROM pg_catalog.pg_class \
         WHERE relname IN ('shop_code_idx', 'shop_parts_idx') \
         ORDER BY relname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["shop_code_idx", "i", "403", "t", "f"]),
                some(&["shop_parts_idx", "I", "403", "f", "t"]),
            ]
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pg_class_links_schema_qualified_composite_type_relations() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE SCHEMA s").await;
    run(&engine, "CREATE SCHEMA mr_sch").await;
    run(
        &engine,
        "CREATE TYPE \"pair.with.dot\" AS (code text, amount int4)",
    )
    .await;
    run(
        &engine,
        "CREATE TYPE s.\"pair.dot\" AS (code text, amount int4)",
    )
    .await;
    run(
        &engine,
        "CREATE TYPE \"s.pair.dot\" AS (code text, amount int4)",
    )
    .await;

    let mut session = engine.connect();
    session
        .simple_query("SET search_path = s")
        .await
        .expect("set type search path");
    session
        .simple_query("CREATE TYPE search_path_pair AS (code text, amount int4)")
        .await
        .expect("create through search path");
    let resolved = session
        .simple_query("SELECT NULL::search_path_pair IS NULL")
        .await
        .expect("resolve unqualified type in the same session")
        .pop()
        .expect("one result");
    assert2::assert!(result_grid(resolved) == vec![some(&["t"])]);

    session
        .simple_query("CREATE TYPE case_pair AS (value int4)")
        .await
        .expect("lower-case type");
    session
        .simple_query("CREATE TYPE \"Case_Pair\" AS (value int4)")
        .await
        .expect("quoted-case type");
    session
        .simple_query(
            "CREATE TABLE case_type_use \
             (lower_value case_pair, upper_value \"Case_Pair\")",
        )
        .await
        .expect("exact-case type lookup");

    session
        .simple_query(
            "CREATE TYPE public.explicit_path_range AS RANGE \
             (SUBTYPE = int4, MULTIRANGE_TYPE_NAME = path_mr)",
        )
        .await
        .expect("unqualified companion uses its own creation path");
    session
        .simple_query(
            "CREATE TYPE range_with_cross_mr AS RANGE \
             (SUBTYPE = int4, MULTIRANGE_TYPE_NAME = mr_sch.\"MR.dot\")",
        )
        .await
        .expect("cross-schema dotted companion");
    session
        .simple_query("CREATE TYPE rename_range AS RANGE (SUBTYPE = int4)")
        .await
        .expect("default companion");
    session
        .simple_query("ALTER TYPE rename_range RENAME TO renamed_base")
        .await
        .expect("rename range base");
    session
        .simple_query("ALTER TYPE mr_sch.\"MR.dot\" RENAME TO \"MR.renamed\"")
        .await
        .expect("rename multirange companion");
    session
        .simple_query("CREATE TYPE lifecycle_pair AS (value int4)")
        .await
        .expect("lifecycle type");
    session
        .simple_query("ALTER TYPE lifecycle_pair ADD ATTRIBUTE label text")
        .await
        .expect("alter through search path");
    session
        .simple_query("DROP TYPE lifecycle_pair")
        .await
        .expect("drop through search path");
    let dropped_catalog_rows = session
        .simple_query(
            "SELECT count(*) FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = 's' AND t.typname = 'lifecycle_pair'",
        )
        .await
        .expect("dropped type catalog visibility")
        .pop()
        .expect("one result");
    assert2::assert!(result_grid(dropped_catalog_rows) == vec![some(&["0"])]);

    session
        .simple_query("CREATE TABLE rowtype_taken (value int4)")
        .await
        .expect("row type occupancy");
    for sql in [
        "CREATE TYPE self_collision AS RANGE \
         (SUBTYPE = int4, MULTIRANGE_TYPE_NAME = self_collision)",
        "CREATE TYPE table_collision AS RANGE \
         (SUBTYPE = int4, MULTIRANGE_TYPE_NAME = rowtype_taken)",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("type-name collision");
        assert2::assert!(error.code == "42710", "{sql}: {error:?}");
    }
    session
        .simple_query("CREATE TYPE blockrange AS RANGE (SUBTYPE = int4)")
        .await
        .expect("range used for companion dependency checks");
    let rename_error = session
        .simple_query("ALTER TYPE blockrange RENAME TO blockmultirange")
        .await
        .expect_err("base cannot take its companion name");
    assert2::assert!(rename_error.code == "42710", "{rename_error:?}");
    let drop_error = session
        .simple_query("DROP TYPE blockmultirange")
        .await
        .expect_err("companion is internally dependent");
    assert2::assert!(drop_error.code == "2BP01", "{drop_error:?}");

    session
        .simple_query("CREATE TYPE rowtype_primary AS (value int4)")
        .await
        .expect("primary type namespace occupant");
    session
        .simple_query(
            "CREATE TYPE rowtype_range AS RANGE \
             (SUBTYPE = int4, MULTIRANGE_TYPE_NAME = rowtype_companion)",
        )
        .await
        .expect("companion type namespace occupant");
    session
        .simple_query("CREATE FOREIGN DATA WRAPPER rowtype_fdw")
        .await
        .expect("foreign wrapper");
    session
        .simple_query("CREATE SERVER rowtype_server FOREIGN DATA WRAPPER rowtype_fdw")
        .await
        .expect("foreign server");
    for (sql, code) in [
        ("CREATE TABLE rowtype_primary (value int4)", "42P07"),
        (
            "CREATE TABLE rowtype_companion (value int4) PARTITION BY RANGE (value)",
            "42710",
        ),
        ("CREATE TABLE rowtype_primary AS SELECT 1 AS value", "42P07"),
        (
            "CREATE VIEW rowtype_companion AS SELECT 1 AS value",
            "42710",
        ),
        (
            "CREATE FOREIGN TABLE rowtype_primary (value int4) SERVER rowtype_server",
            "42P07",
        ),
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("row type cannot replace a user type identity");
        assert2::assert!(error.code == code, "{sql}: {error:?}");
    }
    session
        .simple_query("CREATE TABLE rowtype_rename_source (value int4)")
        .await
        .expect("rename source");
    let rename_error = session
        .simple_query("ALTER TABLE rowtype_rename_source RENAME TO rowtype_companion")
        .await
        .expect_err("rename cannot take a companion identity");
    assert2::assert!(rename_error.code == "42710", "{rename_error:?}");
    let rename_composite = session
        .simple_query("ALTER TABLE rowtype_rename_source RENAME TO rowtype_primary")
        .await
        .expect_err("rename cannot take a composite backing relation identity");
    assert2::assert!(rename_composite.code == "42P07", "{rename_composite:?}");

    // The type namespace is schema-local.
    session
        .simple_query("CREATE TABLE public.rowtype_primary (value int4)")
        .await
        .expect("same type name in another schema");
    session
        .simple_query("CREATE TYPE sequence_enum_name AS ENUM ('value')")
        .await
        .expect("enum sequence collision fixture");
    for (sql, code) in [
        ("CREATE SEQUENCE rowtype_primary", "42P07"),
        ("CREATE SEQUENCE rowtype_range", "42710"),
        ("CREATE SEQUENCE rowtype_companion", "42710"),
        ("CREATE SEQUENCE sequence_enum_name", "42710"),
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("sequence cannot share a user type identity");
        assert2::assert!(error.code == code, "{sql}: {error:?}");
    }

    // An index may share enum/range names, but a composite's backing relkind=c
    // already occupies the relation name.
    session
        .simple_query("CREATE TYPE index_enum_name AS ENUM ('value')")
        .await
        .expect("enum index name");
    session
        .simple_query("CREATE TABLE index_host (value int4)")
        .await
        .expect("index host");
    session
        .simple_query("CREATE INDEX index_enum_name ON index_host (value)")
        .await
        .expect("index shares enum name");
    session
        .simple_query("CREATE INDEX rowtype_range ON index_host (value)")
        .await
        .expect("index shares range name");
    let composite_index = session
        .simple_query("CREATE INDEX rowtype_primary ON index_host (value)")
        .await
        .expect_err("composite backing relation owns the name");
    assert2::assert!(composite_index.code == "42P07", "{composite_index:?}");

    // The inverse direction is the same relation-namespace collision: a new or
    // renamed composite cannot claim an existing sequence/index name.
    session
        .simple_query("CREATE SEQUENCE composite_after_sequence")
        .await
        .expect("sequence before composite");
    session
        .simple_query("CREATE INDEX composite_after_index ON index_host (value)")
        .await
        .expect("index before composite");
    for sql in [
        "CREATE TYPE composite_after_sequence AS (value int4)",
        "CREATE TYPE composite_after_index AS (value int4)",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("composite backing relation collision");
        assert2::assert!(error.code == "42P07", "{sql}: {error:?}");
    }
    session
        .simple_query("CREATE TYPE composite_rename_source AS (value int4)")
        .await
        .expect("composite rename source");
    let composite_rename = session
        .simple_query("ALTER TYPE composite_rename_source RENAME TO composite_after_sequence")
        .await
        .expect_err("renamed composite backing relation collision");
    assert2::assert!(composite_rename.code == "42P07", "{composite_rename:?}");

    // Scalar types have no backing relation, so the inverse creation/rename
    // direction may share an existing sequence or index name.
    session
        .simple_query("CREATE SEQUENCE enum_after_sequence")
        .await
        .expect("sequence before enum");
    session
        .simple_query("CREATE TYPE enum_after_sequence AS ENUM ('value')")
        .await
        .expect("enum shares existing sequence name");
    session
        .simple_query("CREATE INDEX range_after_index ON index_host (value)")
        .await
        .expect("index before range");
    session
        .simple_query("CREATE TYPE range_after_index AS RANGE (SUBTYPE = int4)")
        .await
        .expect("range shares existing index name");
    session
        .simple_query("CREATE SEQUENCE enum_rename_target")
        .await
        .expect("enum rename sequence target");
    session
        .simple_query("CREATE TYPE enum_rename_source AS ENUM ('value')")
        .await
        .expect("enum rename source");
    session
        .simple_query("ALTER TYPE enum_rename_source RENAME TO enum_rename_target")
        .await
        .expect("enum renames onto sequence name");
    session
        .simple_query("CREATE INDEX range_rename_target ON index_host (value)")
        .await
        .expect("range rename index target");
    session
        .simple_query("CREATE TYPE range_rename_source AS RANGE (SUBTYPE = int4)")
        .await
        .expect("range rename source");
    session
        .simple_query("ALTER TYPE range_rename_source RENAME TO range_rename_target")
        .await
        .expect("range renames onto index name");

    session
        .simple_query("CREATE TYPE public.\"INT\" AS (value int4)")
        .await
        .expect("quoted builtin spelling in another schema");
    session
        .simple_query("CREATE TYPE public.int AS (value int4)")
        .await
        .expect("builtin spelling in another schema");
    session
        .simple_query("CREATE TYPE public.int4 AS (value int4)")
        .await
        .expect("catalog type name in another schema");
    session
        .simple_query("SET search_path = public, pg_catalog, s, mr_sch")
        .await
        .expect("put public before pg_catalog");
    let shadows = session
        .simple_query("SELECT NULL::\"INT\" IS NULL, NULL::int IS NULL")
        .await
        .expect("public types shadow later pg_catalog")
        .pop()
        .expect("one result");
    assert2::assert!(result_grid(shadows) == vec![some(&["t", "t"])]);
    assert2::assert!(
        session
            .simple_query("SELECT NULL::pg_catalog.\"INT\"")
            .await
            .is_err()
    );
    session
        .simple_query("SET search_path = pg_catalog, public, s, mr_sch")
        .await
        .expect("system catalog first");
    let system_drop = session
        .simple_query("DROP TYPE int4")
        .await
        .expect_err("catalog type shadows public type for DDL");
    assert2::assert!(system_drop.code == "2BP01", "{system_drop:?}");
    session
        .simple_query("ALTER TYPE int RENAME TO int_alias_renamed")
        .await
        .expect("non-catalog alias still resolves through the path");
    session
        .simple_query("ALTER TYPE public.int_alias_renamed RENAME TO int")
        .await
        .expect("restore alias fixture");
    session
        .simple_query("SET search_path = mr_sch, s, public")
        .await
        .expect("companion lookup path");
    let companions = session
        .simple_query(
            "SELECT NULL::\"MR.renamed\" IS NULL, \
                    NULL::s.rename_multirange IS NULL, \
                    NULL::s.path_mr IS NULL",
        )
        .await
        .expect("structured companion lookup")
        .pop()
        .expect("one result");
    assert2::assert!(result_grid(companions) == vec![some(&["t", "t", "t"])]);
    drop(session);

    assert2::assert!(
        grid(
            &engine,
            "SELECT NULL::\"pair.with.dot\" IS NULL, \
                    NULL::s.\"pair.dot\" IS NULL, \
                    NULL::\"s.pair.dot\" IS NULL",
        )
        .await
            == vec![some(&["t", "t", "t"])]
    );

    let listed = grid(
        &engine,
        "SELECT cn.nspname, c.relname, c.relkind, c.relam = 0, c.reltype = t.oid, \
                c.relfilenode = 0, c.relnatts, tn.nspname, t.typname \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace cn ON cn.oid = c.relnamespace \
         JOIN pg_catalog.pg_type t ON t.typrelid = c.oid \
         JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace \
         WHERE (cn.nspname = 'public' AND c.relname = 'pair.with.dot') OR \
               (cn.nspname = 'public' AND c.relname = 's.pair.dot') OR \
               (cn.nspname = 's' AND \
                c.relname IN ('pair.dot', 'search_path_pair')) \
         ORDER BY cn.nspname, c.relname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&[
                    "public",
                    "pair.with.dot",
                    "c",
                    "t",
                    "t",
                    "t",
                    "2",
                    "public",
                    "pair.with.dot"
                ]),
                some(&[
                    "public",
                    "s.pair.dot",
                    "c",
                    "t",
                    "t",
                    "t",
                    "2",
                    "public",
                    "s.pair.dot"
                ]),
                some(&["s", "pair.dot", "c", "t", "t", "t", "2", "s", "pair.dot"]),
                some(&[
                    "s",
                    "search_path_pair",
                    "c",
                    "t",
                    "t",
                    "t",
                    "2",
                    "s",
                    "search_path_pair"
                ]),
            ]
    );

    let case_type_oids = column(
        &engine,
        "SELECT a.atttypid FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 's' AND c.relname = 'case_type_use' \
         ORDER BY a.attname",
    )
    .await;
    assert2::assert!(case_type_oids.len() == 2 && case_type_oids[0] != case_type_oids[1]);

    let range_catalog = grid(
        &engine,
        "SELECT rn.nspname, r.typname, mn.nspname, m.typname, \
                r.typtype, m.typtype, pr.rngmultitypid = m.oid \
         FROM pg_catalog.pg_range pr \
         JOIN pg_catalog.pg_type r ON r.oid = pr.rngtypid \
         JOIN pg_catalog.pg_namespace rn ON rn.oid = r.typnamespace \
         JOIN pg_catalog.pg_type m ON m.oid = pr.rngmultitypid \
         JOIN pg_catalog.pg_namespace mn ON mn.oid = m.typnamespace \
         WHERE r.typname IN ('explicit_path_range', 'range_with_cross_mr', 'renamed_base') \
         ORDER BY rn.nspname, r.typname",
    )
    .await;
    assert2::assert!(
        range_catalog
            == vec![
                some(&[
                    "public",
                    "explicit_path_range",
                    "s",
                    "path_mr",
                    "r",
                    "m",
                    "t"
                ]),
                some(&[
                    "s",
                    "range_with_cross_mr",
                    "mr_sch",
                    "MR.renamed",
                    "r",
                    "m",
                    "t"
                ]),
                some(&["s", "renamed_base", "s", "rename_multirange", "r", "m", "t"]),
            ]
    );
}

#[tokio::test]
async fn pg_type_classifies_multirange_arrays_as_base_arrays() {
    let engine = SqlEngine::new();
    assert2::assert!(
        grid(
            &engine,
            "SELECT typname, typcategory, typtype FROM pg_catalog.pg_type \
             WHERE typname IN ('int4multirange', '_int4multirange') ORDER BY typname",
        )
        .await
            == vec![
                some(&["_int4multirange", "A", "b"]),
                some(&["int4multirange", "R", "m"]),
            ]
    );
}

/// The column metadata `\d` and every ORM read, including the packed
/// `numeric(p, s)` modifier `format_type` reconstructs the type name from.
#[tokio::test]
async fn pg_attribute_carries_the_metadata_psql_prints() {
    let engine = fixture().await;
    let listed = grid(
        &engine,
        "SELECT a.attname, a.attnum, a.attnotnull, a.atthasdef, a.attidentity, \
                pg_catalog.format_type(a.atttypid, a.atttypmod) \
         FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
         WHERE c.relname = 'shop' AND a.attnum > 0 ORDER BY a.attnum",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["id", "1", "t", "f", "", "integer"]),
                some(&["code", "2", "t", "f", "", "text"]),
                some(&["price", "3", "f", "f", "", "numeric(9,2)"]),
            ]
    );
}

/// `PostgreSQL` 18 records `NOT NULL` in `pg_constraint` alongside the keyed
/// constraints, and `pg_get_constraintdef` rebuilds each one.
#[tokio::test]
async fn pg_constraint_covers_keys_and_not_null() {
    let engine = fixture().await;
    let listed = grid(
        &engine,
        "SELECT con.conname, con.contype, pg_catalog.pg_get_constraintdef(con.oid) \
         FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid \
         WHERE c.relname = 'shop' ORDER BY con.conname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["shop_code_not_null", "n", "NOT NULL code"]),
                some(&["shop_id_not_null", "n", "NOT NULL id"]),
                some(&["shop_pkey", "p", "PRIMARY KEY (id)"]),
            ]
    );
}

/// A constraint that references nothing leaves the `conf*` columns in
/// `PostgreSQL`'s blank spelling: `confrelid` 0, a single space in each of the
/// three `"char"` codes, and NULL attnum arrays.
///
/// That is how a client picks the foreign keys out of a relation's constraint
/// listing.
#[tokio::test]
async fn constraints_without_a_referent_leave_the_referential_columns_blank() {
    let engine = fixture().await;
    let listed = grid(
        &engine,
        "SELECT con.conname, con.confrelid, con.confupdtype, con.confdeltype, \
                con.confmatchtype, con.confkey, con.confdelsetcols \
         FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid \
         WHERE c.relname = 'shop' ORDER BY con.conname",
    )
    .await;
    let blank = |name: &str| {
        let mut row = some(&[name, "0", " ", " ", " "]);
        row.extend([None, None]);
        row
    };
    assert2::assert!(
        listed
            == vec![
                blank("shop_code_not_null"),
                blank("shop_id_not_null"),
                blank("shop_pkey"),
            ]
    );
}

/// `\d` reads one `pg_constraint` listing per relation, so a foreign key has to
/// show up beside the keyed and `NOT NULL` constraints of the same table, under
/// its own `contype` and with its own rebuilt definition.
#[tokio::test]
async fn pg_constraint_lists_a_foreign_key_beside_the_other_constraint_kinds() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE TABLE sale (id int4 PRIMARY KEY, \
         shop_id int4 NOT NULL REFERENCES shop (id) ON DELETE CASCADE)",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT con.conname, con.contype, pg_catalog.pg_get_constraintdef(con.oid) \
         FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid \
         WHERE c.relname = 'sale' ORDER BY con.conname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["sale_id_not_null", "n", "NOT NULL id"]),
                some(&["sale_pkey", "p", "PRIMARY KEY (id)"]),
                some(&[
                    "sale_shop_id_fkey",
                    "f",
                    "FOREIGN KEY (shop_id) REFERENCES shop(id) ON DELETE CASCADE",
                ]),
                some(&["sale_shop_id_not_null", "n", "NOT NULL shop_id"]),
            ]
    );
}

/// `pg_get_indexdef` rebuilds the `CREATE INDEX` statement, schema-qualified.
#[tokio::test]
async fn pg_get_indexdef_rebuilds_the_create_statement() {
    let engine = fixture().await;
    let listed = column(
        &engine,
        "SELECT pg_catalog.pg_get_indexdef(c.oid) FROM pg_catalog.pg_class c \
         WHERE c.relname IN ('shop_pkey', 'shop_code_idx') ORDER BY c.relname",
    )
    .await;
    assert2::assert!(
        listed
            == some(&[
                "CREATE INDEX shop_code_idx ON public.shop USING btree (code)",
                "CREATE UNIQUE INDEX shop_pkey ON public.shop USING btree (id)",
            ])
    );
}

/// `pg_get_viewdef` reaches a view by name and by oid, and both spellings of
/// the pretty flag answer `PostgreSQL`'s layout.
#[tokio::test]
async fn pg_get_viewdef_answers_by_name_and_by_oid() {
    let engine = fixture().await;
    let by_name = column(&engine, "SELECT pg_catalog.pg_get_viewdef('shop_v')").await;
    let by_oid = column(
        &engine,
        "SELECT pg_catalog.pg_get_viewdef(c.oid) FROM pg_catalog.pg_class c \
         WHERE c.relname = 'shop_v'",
    )
    .await;
    assert2::assert!(by_name == some(&[" SELECT id,\n    code\n   FROM shop;"]));
    assert2::assert!(by_name == by_oid);

    // A relation that is not a view is PostgreSQL's literal `Not a view`.
    let not_a_view = column(&engine, "SELECT pg_catalog.pg_get_viewdef('shop')").await;
    assert2::assert!(not_a_view == some(&["Not a view"]));
}

/// A sequence is a relation. `\ds` lists it out of `pg_class`, and
/// `pg_sequence` and `information_schema.sequences` describe its parameters.
#[tokio::test]
async fn sequences_appear_as_relations_and_in_their_own_catalogs() {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE SEQUENCE counter START 5 INCREMENT BY 2 MAXVALUE 99 CYCLE",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'counter'",
    )
    .await;
    assert2::assert!(listed == vec![some(&["counter", "S"])]);

    let described = grid(
        &engine,
        "SELECT s.seqstart, s.seqincrement, s.seqmax, s.seqcycle \
         FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid \
         WHERE c.relname = 'counter'",
    )
    .await;
    assert2::assert!(described == vec![some(&["5", "2", "99", "t"])]);

    let standard = grid(
        &engine,
        "SELECT sequence_name, data_type, start_value, increment, cycle_option \
         FROM information_schema.sequences WHERE sequence_name = 'counter'",
    )
    .await;
    assert2::assert!(standard == vec![some(&["counter", "bigint", "5", "2", "YES"])]);
}

/// `COMMENT ON` text reaches `pg_description`, `obj_description` and
/// `col_description`. A cleared comment removes all three answers.
#[tokio::test]
async fn comments_reach_pg_description_and_the_description_functions() {
    let engine = fixture().await;
    run(&engine, "COMMENT ON TABLE shop IS 'the shop'").await;
    run(&engine, "COMMENT ON COLUMN shop.code IS 'the code'").await;

    let described = grid(
        &engine,
        "SELECT pg_catalog.obj_description(c.oid), pg_catalog.col_description(c.oid, 2) \
         FROM pg_catalog.pg_class c WHERE c.relname = 'shop'",
    )
    .await;
    assert2::assert!(described == vec![some(&["the shop", "the code"])]);

    let counted = column(
        &engine,
        "SELECT count(*) FROM pg_catalog.pg_description d \
         JOIN pg_catalog.pg_class c ON c.oid = d.objoid WHERE c.relname = 'shop'",
    )
    .await;
    assert2::assert!(counted == some(&["2"]));

    run(&engine, "COMMENT ON TABLE shop IS NULL").await;
    let cleared = column(
        &engine,
        "SELECT pg_catalog.obj_description(c.oid) IS NULL FROM pg_catalog.pg_class c \
         WHERE c.relname = 'shop'",
    )
    .await;
    assert2::assert!(cleared == some(&["t"]));
}

/// The SQL-standard constraint views describe the same constraints
/// `pg_constraint` does, under the standard's names.
#[tokio::test]
async fn information_schema_describes_constraints_and_views() {
    let engine = fixture().await;
    let constraints = grid(
        &engine,
        "SELECT constraint_name, constraint_type, is_deferrable, initially_deferred \
         FROM information_schema.table_constraints \
         WHERE table_name = 'shop' AND constraint_type = 'PRIMARY KEY'",
    )
    .await;
    assert2::assert!(constraints == vec![some(&["shop_pkey", "PRIMARY KEY", "NO", "NO"])]);

    let usage = grid(
        &engine,
        "SELECT constraint_name, column_name, ordinal_position \
         FROM information_schema.key_column_usage WHERE table_name = 'shop'",
    )
    .await;
    assert2::assert!(usage == vec![some(&["shop_pkey", "id", "1"])]);

    let views = grid(
        &engine,
        "SELECT table_name, check_option, is_updatable, is_insertable_into \
         FROM information_schema.views WHERE table_name = 'shop_v'",
    )
    .await;
    assert2::assert!(views == vec![some(&["shop_v", "NONE", "YES", "YES"])]);
}

/// [`fixture`] plus one view per shape the updatability analysis distinguishes,
/// and the two non-updatable relation kinds `regclass` also accepts.
///
/// The values every test below expects were read off `PostgreSQL` 18.4 for this
/// exact schema, so a case may only be retyped from a fresh oracle reading.
async fn updatable_view_fixture() -> SqlEngine {
    let engine = fixture().await;
    for ddl in [
        // Auto-updatable and nothing else: the baseline.
        "CREATE VIEW shop_d AS SELECT DISTINCT code FROM shop",
        "CREATE VIEW shop_g AS SELECT code, count(*) FROM shop GROUP BY code",
        // Only an expression is projected, so no column can be assigned to and
        // the view admits DELETE alone.
        "CREATE VIEW shop_e AS SELECT upper(code) AS u FROM shop",
        // One assignable column is enough to admit INSERT and UPDATE too, even
        // beside an expression that is not assignable.
        "CREATE VIEW shop_m AS SELECT id, upper(code) AS u FROM shop",
        // A view over a DELETE-only view is held to the same limit.
        "CREATE VIEW shop_n AS SELECT u FROM shop_e",
        "CREATE VIEW shop_co AS SELECT id, code FROM shop WHERE id > 0 \
         WITH CASCADED CHECK OPTION",
        "CREATE VIEW shop_lo AS SELECT id, code FROM shop WHERE id > 0 \
         WITH LOCAL CHECK OPTION",
        "CREATE SEQUENCE shop_seq",
    ] {
        run(&engine, ddl).await;
    }
    engine
}

/// `information_schema.views` reports the declared `WITH CHECK OPTION` level and
/// the standard's two updatability columns.
///
/// `is_updatable` is not "is this view auto-updatable": the standard asks
/// whether a row can be both updated *and* deleted through the view, so a view
/// projecting only expressions — which admits DELETE and nothing else — reports
/// `NO` despite being a perfectly simple single-relation `SELECT`.
#[tokio::test]
async fn the_standard_view_catalog_reports_check_option_and_updatability() {
    let engine = updatable_view_fixture().await;
    let listed = grid(
        &engine,
        "SELECT table_name, check_option, is_updatable, is_insertable_into \
         FROM information_schema.views WHERE table_schema = 'public' ORDER BY table_name",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["shop_co", "CASCADED", "YES", "YES"]),
                some(&["shop_d", "NONE", "NO", "NO"]),
                some(&["shop_e", "NONE", "NO", "NO"]),
                some(&["shop_g", "NONE", "NO", "NO"]),
                some(&["shop_lo", "LOCAL", "YES", "YES"]),
                some(&["shop_m", "NONE", "YES", "YES"]),
                some(&["shop_n", "NONE", "NO", "NO"]),
                some(&["shop_v", "NONE", "YES", "YES"]),
            ]
    );
}

/// `pg_relation_is_updatable` reports the write commands a relation admits as
/// `PostgreSQL`'s bitmask: 4 UPDATE, 8 INSERT, 16 DELETE.
#[tokio::test]
async fn the_relation_updatability_bitmask_matches_postgresql() {
    // A table admits all three (28). A view admits DELETE alone (16) when its
    // select list assigns to nothing, and none of the three (0) when its body is
    // too complex to rewrite at all. An index or a sequence is never updatable.
    const CASES: [(&str, &str); 11] = [
        ("shop", "28"),
        ("shop_v", "28"),
        ("shop_co", "28"),
        ("shop_lo", "28"),
        ("shop_m", "28"),
        ("shop_e", "16"),
        ("shop_n", "16"),
        ("shop_d", "0"),
        ("shop_g", "0"),
        ("shop_code_idx", "0"),
        ("shop_seq", "0"),
    ];
    let engine = updatable_view_fixture().await;
    let mut answered = Vec::new();
    for (relation, _) in CASES {
        let sql = format!("SELECT pg_catalog.pg_relation_is_updatable('{relation}', false)");
        answered.push((relation, column(&engine, &sql).await));
    }
    let expected = CASES
        .map(|(relation, mask)| (relation, some(&[mask])))
        .to_vec();
    assert2::assert!(answered == expected);
}

/// `pg_column_is_updatable` asks the same question of one column, which is what
/// separates the two expression views from each other.
#[tokio::test]
async fn column_updatability_follows_the_projected_column_to_its_table() {
    // A table settles the question before it reads the column number, so even a
    // number past the end of its column list answers true — but a system column
    // (attnum <= 0) never does.
    const CASES: [(&str, i32, &str); 12] = [
        ("shop", 1, "t"),
        ("shop", 99, "t"),
        ("shop", 0, "f"),
        ("shop", -1, "f"),
        ("shop_v", 2, "t"),
        // `id` is assignable; `upper(code)` is not.
        ("shop_m", 1, "t"),
        ("shop_m", 2, "f"),
        ("shop_e", 1, "f"),
        // The column exists in the view above, but not in the one below it.
        ("shop_n", 1, "f"),
        ("shop_d", 1, "f"),
        ("shop_g", 1, "f"),
        ("shop_seq", 1, "f"),
    ];
    let engine = updatable_view_fixture().await;
    let mut answered = Vec::new();
    for (relation, attnum, _) in CASES {
        let sql = format!(
            "SELECT pg_catalog.pg_column_is_updatable('{relation}', {attnum}::int2, false)"
        );
        answered.push((relation, attnum, column(&engine, &sql).await));
    }
    let expected = CASES
        .map(|(relation, attnum, held)| (relation, attnum, some(&[held])))
        .to_vec();
    assert2::assert!(answered == expected);
}

/// Both predicates are strict, and both tolerate an oid no relation answers to:
/// `999999::regclass` is a legal value, so `PostgreSQL` reaches the function body
/// and finds nothing to open rather than raising 42P01.
#[tokio::test]
async fn the_updatability_predicates_are_strict_and_tolerate_a_stale_oid() {
    let engine = updatable_view_fixture().await;
    let answered = grid(
        &engine,
        "SELECT pg_catalog.pg_relation_is_updatable(NULL, false) IS NULL, \
                pg_catalog.pg_relation_is_updatable('shop_v', NULL) IS NULL, \
                pg_catalog.pg_column_is_updatable(NULL, 1::int2, false) IS NULL, \
                pg_catalog.pg_column_is_updatable('shop_v', NULL, false) IS NULL, \
                pg_catalog.pg_column_is_updatable('shop_v', 1::int2, NULL) IS NULL, \
                pg_catalog.pg_relation_is_updatable(999999, false), \
                pg_catalog.pg_column_is_updatable(999999, 1::int2, false)",
    )
    .await;
    assert2::assert!(answered == vec![some(&["t", "t", "t", "t", "t", "0", "f"])]);
}

/// The identity and formatting functions clients call before anything else.
#[tokio::test]
async fn identity_and_formatting_functions_answer() {
    let engine = SqlEngine::new();
    let answers = grid(
        &engine,
        "SELECT current_schemas(true), current_schemas(false), \
                pg_catalog.pg_encoding_to_char(6), pg_catalog.pg_char_to_encoding('UTF8'), \
                pg_catalog.pg_size_pretty(10240::int8), pg_catalog.pg_backend_pid() > 0, \
                pg_catalog.pg_is_in_recovery(), pg_catalog.pg_get_userbyid(10)",
    )
    .await;
    assert2::assert!(
        answers
            == vec![some(&[
                "{pg_catalog,public}",
                "{public}",
                "UTF8",
                "6",
                "10 kB",
                "t",
                "f",
                "postgres",
            ])]
    );
}

/// `pg_postmaster_start_time()` must be in the past. A lazily captured instant
/// would make every uptime query report a negative age.
#[tokio::test]
async fn the_postmaster_start_time_precedes_the_statement() {
    let engine = SqlEngine::new();
    let answer = column(
        &engine,
        "SELECT pg_catalog.pg_postmaster_start_time() <= now()",
    )
    .await;
    assert2::assert!(answer == some(&["t"]));
}

/// The `has_*_privilege` family answers for the owner. It rejects a privilege
/// name `PostgreSQL` does not know with 22023, and it does not answer false.
#[tokio::test]
async fn privilege_tests_answer_for_the_owner_and_reject_unknown_privileges() {
    let engine = fixture().await;
    let held = grid(
        &engine,
        "SELECT pg_catalog.has_table_privilege('shop', 'SELECT'), \
                pg_catalog.has_table_privilege('shop', 'INSERT'), \
                pg_catalog.has_schema_privilege('public', 'USAGE')",
    )
    .await;
    assert2::assert!(held == vec![some(&["t", "t", "t"])]);

    let error = engine
        .connect()
        .simple_query("SELECT pg_catalog.has_table_privilege('shop', 'NONESUCH')")
        .await
        .expect_err("unrecognized privilege is an error");
    assert2::assert!(error.code == "22023", "{error:?}");

    let missing = engine
        .connect()
        .simple_query("SELECT pg_catalog.has_table_privilege('nosuchtable', 'SELECT')")
        .await
        .expect_err("missing relation is an error");
    assert2::assert!(missing.code == "42P01", "{missing:?}");
}

/// A relation size resolves its argument, so a missing relation is 42P01, and
/// it reports zero. Crabka keeps no per-relation storage accounting.
#[tokio::test]
async fn relation_sizes_resolve_their_argument_and_report_zero() {
    let engine = fixture().await;
    let sizes = grid(
        &engine,
        "SELECT pg_catalog.pg_relation_size('shop'), pg_catalog.pg_total_relation_size('shop')",
    )
    .await;
    assert2::assert!(sizes == vec![some(&["0", "0"])]);

    let missing = engine
        .connect()
        .simple_query("SELECT pg_catalog.pg_relation_size('nosuchtable')")
        .await
        .expect_err("missing relation is an error");
    assert2::assert!(missing.code == "42P01", "{missing:?}");
}

/// `pg_attribute.attstorage` is the column type's own storage class, which is
/// what `\d+` prints in its Storage column.
///
/// Every expectation is `pg_type.typstorage` read off the pinned
/// `PostgreSQL` 18.4 catalog, not inferred from whether the type looks fixed-length — two
/// groups defeat that intuition. `inet` and `cidr` are `main` alongside
/// `numeric`; and of the geometric types only the fixed ones are `plain`,
/// while `path` is varlena and so `extended`.
#[tokio::test]
async fn attstorage_reports_each_types_storage_class() {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE stg (\
             p_int int, p_bool bool, p_ts timestamptz, p_uuid uuid, p_money money, \
             p_point point, p_oid oid, \
             m_numeric numeric, m_inet inet, m_cidr cidr, \
             x_text text, x_varchar varchar(9), x_bytea bytea, x_jsonb jsonb, \
             x_array int[], x_path path)",
    )
    .await;

    let rows = grid(
        &engine,
        "SELECT a.attname, a.attstorage FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         WHERE c.relname = 'stg' AND a.attnum > 0 ORDER BY a.attnum",
    )
    .await;

    let got: Vec<(String, String)> = rows
        .into_iter()
        .map(|row| {
            (
                row[0].clone().expect("attname"),
                row[1].clone().expect("attstorage"),
            )
        })
        .collect();
    let expected: Vec<(String, String)> = [
        ("p_int", "p"),
        ("p_bool", "p"),
        ("p_ts", "p"),
        ("p_uuid", "p"),
        ("p_money", "p"),
        ("p_point", "p"),
        ("p_oid", "p"),
        ("m_numeric", "m"),
        ("m_inet", "m"),
        ("m_cidr", "m"),
        ("x_text", "x"),
        ("x_varchar", "x"),
        ("x_bytea", "x"),
        ("x_jsonb", "x"),
        ("x_array", "x"),
        ("x_path", "x"),
    ]
    .into_iter()
    .map(|(name, class)| (name.to_owned(), class.to_owned()))
    .collect();
    assert!(got == expected);
}

/// A catalog that `PostgreSQL` implements as a SQL view must report `relkind`
/// `v`, not `r`.
///
/// `misc_sanity` asks for `pg_catalog` relations with `relkind = 'r'` that have
/// no primary key, and a view answering `r` joins that list wrongly. That is how
/// `pg_matviews` was caught: it was added to the catalog relation list without
/// being added to the view list beside it, so the two lists drifted. Naming
/// every view here means the next one to drift fails a test rather than a
/// certification run.
#[tokio::test]
async fn a_catalog_implemented_as_a_view_does_not_claim_to_be_a_table() {
    let engine = SqlEngine::new();
    for name in [
        "pg_indexes",
        "pg_locks",
        "pg_matviews",
        "pg_policies",
        "pg_replication_slots",
        "pg_settings",
        "pg_shmem_allocations_numa",
        "pg_stat_activity",
        "pg_tables",
        "pg_views",
    ] {
        let sql = format!(
            "SELECT relkind FROM pg_class WHERE relname = '{name}' AND relnamespace = 'pg_catalog'::regnamespace"
        );
        assert!(
            column(&engine, &sql).await == vec![Some("v".to_owned())],
            "{name}"
        );
    }

    // The base catalogs beside them must still answer `r`, or the fix would be
    // "call everything a view".
    for name in ["pg_class", "pg_attribute", "pg_proc", "pg_type"] {
        let sql = format!(
            "SELECT relkind FROM pg_class WHERE relname = '{name}' AND relnamespace = 'pg_catalog'::regnamespace"
        );
        assert!(
            column(&engine, &sql).await == vec![Some("r".to_owned())],
            "{name}"
        );
    }
}

/// A deparsed identifier is quoted exactly when `PostgreSQL` quotes it, which
/// takes both halves of the test: a safe character shape *and* a word that is
/// either no keyword at all or one in the `UNRESERVED` category.
///
/// The asymmetry is the point of the case. `set` is unreserved and stays bare;
/// `values` and `exists` are not, so they are quoted. A renderer that tested
/// character shape alone would leave all three bare, and one that quoted every
/// keyword would quote `set` — so a case naming only one of them cannot tell
/// the two failures apart, and this one names both.
///
/// Every expectation below was captured from `postgres:18.4`, which answers
/// `set | "values" | "exists" | plain` for `quote_ident` on the four names and
/// prints the same spellings inside each definition it deparses.
#[tokio::test]
async fn a_deparsed_keyword_identifier_is_quoted_only_where_postgres_quotes_it() {
    let engine = SqlEngine::new();
    run(
        &engine,
        r#"CREATE TABLE kwt ("set" int4, "values" int4, "exists" int4, plain int4)"#,
    )
    .await;
    run(
        &engine,
        r#"CREATE UNIQUE INDEX kwt_ix ON kwt ("values", "set")"#,
    )
    .await;
    run(
        &engine,
        r#"ALTER TABLE kwt ADD CONSTRAINT kwt_ck CHECK ("values" > 0)"#,
    )
    .await;
    run(
        &engine,
        r#"CREATE VIEW kwv AS SELECT "set", "values", plain FROM kwt"#,
    )
    .await;

    // The SQL function and the deparser have to agree, because they answer the
    // same question; the bug this pins was two renderers disagreeing.
    assert!(
        grid(
            &engine,
            "SELECT quote_ident('set'), quote_ident('values'), \
                    quote_ident('exists'), quote_ident('plain')",
        )
        .await
            == vec![some(&["set", r#""values""#, r#""exists""#, "plain"])]
    );

    assert!(
        column(&engine, "SELECT pg_get_indexdef('kwt_ix'::regclass)").await
            == some(&[r#"CREATE UNIQUE INDEX kwt_ix ON public.kwt USING btree ("values", set)"#])
    );
    // `pg_indexes.indexdef` is the same text by another road, and psql's `\di`
    // reads it rather than the function.
    assert!(
        column(
            &engine,
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'kwt_ix'",
        )
        .await
            == some(&[r#"CREATE UNIQUE INDEX kwt_ix ON public.kwt USING btree ("values", set)"#])
    );
    assert!(
        column(
            &engine,
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'kwt_ck'",
        )
        .await
            == some(&[r#"CHECK (("values" > 0))"#])
    );
    assert!(
        column(&engine, "SELECT pg_get_viewdef('kwv'::regclass)").await
            == some(&[" SELECT set,\n    \"values\",\n    plain\n   FROM kwt;"])
    );
}
