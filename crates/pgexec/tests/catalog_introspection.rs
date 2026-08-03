//! F-2: the `pg_catalog`/`information_schema` breadth `psql`'s `\d` family and
//! ORM preambles depend on.
//!
//! These tests exercise that breadth through the SQL session, the way a client
//! reaches it.

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
    match run(engine, sql).await {
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
        "pg_catalog.pg_depend",
        "pg_catalog.pg_rewrite",
        "pg_catalog.pg_trigger",
        "pg_catalog.pg_sequence",
        "pg_catalog.pg_collation",
        "pg_catalog.pg_enum",
        "pg_catalog.pg_range",
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
                Some("2746".into()),
                Some("3389".into()),
                Some("2785".into()),
                Some("2786".into()),
                Some("2787".into()),
                Some("2281".into()),
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
        listed == vec![some(&["4402", "koi8_r_to_mic", "11", "10", "22", "7", "4302", "t"])]
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
    assert2::assert!(count == some(&["52"]));
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

/// The column metadata `\d` and every ORM read.
///
/// This includes the packed `numeric(p, s)` modifier that `format_type`
/// reconstructs the type name from.
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

/// A view that is not a simple single-relation `SELECT` is not auto-updatable,
/// which is the test `PostgreSQL`'s `is_updatable` column reports.
#[tokio::test]
async fn only_simple_views_report_as_auto_updatable() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE VIEW shop_d AS SELECT DISTINCT code FROM shop",
    )
    .await;
    run(
        &engine,
        "CREATE VIEW shop_g AS SELECT code, count(*) FROM shop GROUP BY code",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT table_name, is_updatable FROM information_schema.views \
         WHERE table_name IN ('shop_v', 'shop_d', 'shop_g') ORDER BY table_name",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["shop_d", "NO"]),
                some(&["shop_g", "NO"]),
                some(&["shop_v", "YES"]),
            ]
    );
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
