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

fn rows(result: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("valid text cell"))
}

#[tokio::test]
async fn pg_catalog_exposes_user_tables_columns_types_and_indexes() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE users (id int4, name text)").await;
    run(&engine, "CREATE INDEX users_name_idx ON users (name)").await;

    let class_rows = run(
        &engine,
        "SELECT c.relname, c.relkind, c.relhasindex \
         FROM pg_catalog.pg_class c \
         WHERE c.relname = 'users'",
    )
    .await;
    assert_eq!(rows(&class_rows).len(), 1);
    assert_eq!(
        cell_text(rows(&class_rows)[0][0].as_ref()),
        Some("users".into())
    );
    assert_eq!(
        cell_text(rows(&class_rows)[0][1].as_ref()),
        Some("r".into())
    );
    assert_eq!(
        cell_text(rows(&class_rows)[0][2].as_ref()),
        Some("t".into())
    );

    let attribute_rows = run(
        &engine,
        "SELECT a.attname, t.typname \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid \
         JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
         WHERE c.relname = 'users' \
         ORDER BY a.attnum",
    )
    .await;
    let attributes: Vec<_> = rows(&attribute_rows)
        .iter()
        .map(|row| (cell_text(row[0].as_ref()), cell_text(row[1].as_ref())))
        .collect();
    assert_eq!(
        attributes,
        vec![
            (Some("id".into()), Some("int4".into())),
            (Some("name".into()), Some("text".into())),
        ]
    );

    let index_rows = run(
        &engine,
        "SELECT ci.relname, i.indisunique, i.indkey \
         FROM pg_catalog.pg_class ct \
         JOIN pg_catalog.pg_index i ON i.indrelid = ct.oid \
         JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid \
         WHERE ct.relname = 'users'",
    )
    .await;
    assert_eq!(rows(&index_rows).len(), 1);
    assert_eq!(
        cell_text(rows(&index_rows)[0][0].as_ref()),
        Some("users_name_idx".into())
    );
    assert_eq!(
        cell_text(rows(&index_rows)[0][1].as_ref()),
        Some("f".into())
    );
    assert_eq!(
        cell_text(rows(&index_rows)[0][2].as_ref()),
        Some("2".into())
    );
}

#[tokio::test]
async fn pg_settings_reports_effective_session_values() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("SET application_name = 'sqlx-test'")
        .await
        .expect("set application_name");

    let result = session
        .simple_query(
            "SELECT name, setting, vartype \
             FROM pg_catalog.pg_settings \
             WHERE name = 'application_name'",
        )
        .await
        .expect("select pg_settings")
        .into_iter()
        .next()
        .expect("one result");

    assert_eq!(rows(&result).len(), 1);
    assert_eq!(
        cell_text(rows(&result)[0][0].as_ref()),
        Some("application_name".into())
    );
    assert_eq!(
        cell_text(rows(&result)[0][1].as_ref()),
        Some("sqlx-test".into())
    );
    assert_eq!(
        cell_text(rows(&result)[0][2].as_ref()),
        Some("string".into())
    );
}

#[tokio::test]
async fn pg_catalog_helper_functions_cover_client_preambles() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT current_schema(), current_database(), version(), \
         pg_catalog.format_type(23, -1), pg_catalog.pg_table_is_visible(1259)",
    )
    .await;

    assert_eq!(
        cell_text(rows(&result)[0][0].as_ref()),
        Some("public".into())
    );
    assert_eq!(
        cell_text(rows(&result)[0][1].as_ref()),
        Some("postgres".into())
    );
    assert_eq!(
        cell_text(rows(&result)[0][2].as_ref()),
        Some("PostgreSQL 18-compatible Crabka".into())
    );
    assert_eq!(cell_text(rows(&result)[0][3].as_ref()), Some("int4".into()));
    assert_eq!(cell_text(rows(&result)[0][4].as_ref()), Some("t".into()));
}

#[tokio::test]
async fn role_ddl_set_role_and_pg_roles_are_starter_supported() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    session
        .simple_query("CREATE ROLE analysts")
        .await
        .expect("create role");
    session
        .simple_query("CREATE USER app_user")
        .await
        .expect("create user");

    let roles = session
        .simple_query(
            "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles \
             WHERE rolname = 'analysts' OR rolname = 'app_user' ORDER BY rolname",
        )
        .await
        .expect("select pg_roles")
        .into_iter()
        .next()
        .expect("one result");
    let role_rows: Vec<_> = rows(&roles)
        .iter()
        .map(|row| (cell_text(row[0].as_ref()), cell_text(row[1].as_ref())))
        .collect();
    assert_eq!(
        role_rows,
        vec![
            (Some("analysts".into()), Some("f".into())),
            (Some("app_user".into()), Some("t".into())),
        ]
    );

    let switched = session
        .simple_query(
            "SET ROLE analysts; SELECT current_user, session_user; RESET ROLE; SELECT current_user",
        )
        .await
        .expect("set/reset role");
    assert_eq!(
        cell_text(rows(&switched[1])[0][0].as_ref()),
        Some("analysts".into())
    );
    assert_eq!(
        cell_text(rows(&switched[1])[0][1].as_ref()),
        Some("public".into())
    );
    assert_eq!(
        cell_text(rows(&switched[3])[0][0].as_ref()),
        Some("public".into())
    );

    session
        .simple_query("DROP ROLE analysts; DROP USER app_user")
        .await
        .expect("drop roles");
}

#[tokio::test]
async fn grant_and_revoke_record_acl_metadata_without_enforcement() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE docs (id int4); CREATE ROLE reader")
        .await
        .expect("fixtures");

    session
        .simple_query("GRANT SELECT ON TABLE docs TO reader")
        .await
        .expect("grant records metadata");
    session
        .simple_query("REVOKE SELECT ON TABLE docs FROM reader")
        .await
        .expect("revoke removes metadata");
}

async fn engine_with_information_schema_fixtures() -> SqlEngine {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE users (\
         id serial, \
         handle varchar(12) NOT NULL, \
         code character(2), \
         uid uuid, \
         name text DEFAULT 'anon')",
    )
    .await;
    run(&engine, "CREATE FOREIGN DATA WRAPPER kafka_fdw").await;
    run(
        &engine,
        "CREATE SERVER kafka_srv FOREIGN DATA WRAPPER kafka_fdw",
    )
    .await;
    run(
        &engine,
        "CREATE FOREIGN TABLE remote_events (payload text) SERVER kafka_srv",
    )
    .await;
    engine
}

#[tokio::test]
async fn information_schema_exposes_schemata_and_tables() {
    let engine = engine_with_information_schema_fixtures().await;
    let schemata = run(
        &engine,
        "SELECT schema_name \
         FROM information_schema.schemata \
         ORDER BY schema_name",
    )
    .await;
    let schema_names: Vec<_> = rows(&schemata)
        .iter()
        .map(|row| cell_text(row[0].as_ref()))
        .collect();
    assert_eq!(
        schema_names,
        vec![
            Some("information_schema".into()),
            Some("pg_catalog".into()),
            Some("public".into()),
        ]
    );

    let table_rows = run(
        &engine,
        "SELECT table_name, table_type \
         FROM information_schema.tables \
         WHERE table_schema = 'public' \
         ORDER BY table_name",
    )
    .await;
    let tables: Vec<_> = rows(&table_rows)
        .iter()
        .map(|row| (cell_text(row[0].as_ref()), cell_text(row[1].as_ref())))
        .collect();
    assert_eq!(
        tables,
        vec![
            (Some("remote_events".into()), Some("FOREIGN".into())),
            (Some("users".into()), Some("BASE TABLE".into())),
        ]
    );
}

#[tokio::test]
async fn information_schema_exposes_columns_with_type_nullability_and_defaults() {
    let engine = engine_with_information_schema_fixtures().await;
    let column_rows = run(
        &engine,
        "SELECT column_name, ordinal_position, data_type, is_nullable, column_default \
         FROM information_schema.columns \
         WHERE table_name = 'users' \
         ORDER BY ordinal_position",
    )
    .await;
    let columns: Vec<_> = rows(&column_rows)
        .iter()
        .map(|row| {
            (
                cell_text(row[0].as_ref()),
                cell_text(row[1].as_ref()),
                cell_text(row[2].as_ref()),
                cell_text(row[3].as_ref()),
                cell_text(row[4].as_ref()),
            )
        })
        .collect();
    assert_eq!(
        columns,
        vec![
            (
                Some("id".into()),
                Some("1".into()),
                Some("integer".into()),
                Some("NO".into()),
                Some("nextval('users_id_seq'::regclass)".into()),
            ),
            (
                Some("handle".into()),
                Some("2".into()),
                Some("character varying".into()),
                Some("NO".into()),
                None,
            ),
            (
                Some("code".into()),
                Some("3".into()),
                Some("character".into()),
                Some("YES".into()),
                None,
            ),
            (
                Some("uid".into()),
                Some("4".into()),
                Some("uuid".into()),
                Some("YES".into()),
                None,
            ),
            (
                Some("name".into()),
                Some("5".into()),
                Some("text".into()),
                Some("YES".into()),
                Some("'anon'::text".into()),
            ),
        ]
    );
}
