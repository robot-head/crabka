//! Column-level `GRANT`/`REVOKE`, and the `pg_init_privs` catalog `pg_dump`
//! reads alongside it.
//!
//! Both are the upstream `init_privs` test. It asks for three things: that
//! `pg_init_privs` exists and is non-empty, and that
//! `GRANT SELECT (prosrc) ON pg_proc TO CURRENT_USER` and
//! `GRANT SELECT (rolname, rolsuper) ON pg_authid TO CURRENT_USER` are ordinary
//! successful statements. Every one of the three used to fail.
//!
//! The column list hangs off each privilege and not off the statement, so
//! `GRANT SELECT (a), UPDATE (b) ON t TO r` means different columns per
//! privilege. That is pinned here rather than only in the parser, because the
//! executor is where the two lists could be crossed.
//!
//! **What a column grant does not do.** It is stored, and
//! `information_schema.column_privileges` and `pg_attribute.attacl` report it,
//! but it does not admit a read. The read permit is taken before the query's
//! projection is known, so a role holding only column grants is refused the
//! relation where `PostgreSQL` would let it read the granted columns. That is
//! narrower than `PostgreSQL` and fails closed, and
//! `a_column_grant_is_recorded_but_does_not_widen_a_read` states it so that
//! widening it has to be deliberate.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row a query returns, each row as its cells' text.
async fn rows_of(session: &mut SqlSession, sql: &str) -> Vec<Vec<String>> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|cell| cell_text(cell.as_ref())).collect())
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    let rows = rows_of(session, sql).await;
    let [row] = rows.as_slice() else {
        panic!("expected one row from {sql}, got {rows:?}");
    };
    let [cell] = row.as_slice() else {
        panic!("expected one column from {sql}, got {row:?}");
    };
    cell.clone()
}

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

/// The `information_schema.column_privileges` rows for one relation, as
/// `(grantee, column, privilege)`, sorted so the comparison is stable.
async fn column_grants(session: &mut SqlSession, relation: &str) -> Vec<Vec<String>> {
    let mut rows = rows_of(
        session,
        &format!(
            "SELECT grantee, column_name, privilege_type
             FROM information_schema.column_privileges
             WHERE table_name = '{relation}'"
        ),
    )
    .await;
    rows.sort();
    rows
}

// ------------------------------------------------------------ init_privs ---

/// The upstream `init_privs` file, statement for statement.
#[tokio::test]
async fn the_upstream_init_privs_statements_all_succeed() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    assert!(scalar(&mut session, "SELECT count(*) > 0 FROM pg_init_privs").await == "t");

    run(&mut session, "GRANT SELECT ON pg_proc TO CURRENT_USER").await;
    run(
        &mut session,
        "GRANT SELECT (prosrc) ON pg_proc TO CURRENT_USER",
    )
    .await;
    run(
        &mut session,
        "GRANT SELECT (rolname, rolsuper) ON pg_authid TO CURRENT_USER",
    )
    .await;
}

/// `pg_init_privs` describes bootstrap state and nothing else: one row per
/// catalog relation, all of it from `initdb`, all of it a `PUBLIC` `SELECT`,
/// and no row for a relation a user created.
#[tokio::test]
async fn pg_init_privs_describes_only_the_bootstrap_catalogs() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE doc (id int4)").await;

    let shapes = rows_of(
        &mut session,
        "SELECT DISTINCT privtype, objsubid, initprivs::text FROM pg_init_privs",
    )
    .await;
    assert!(
        shapes
            == vec![vec![
                "i".to_string(),
                "0".to_string(),
                "{=r/postgres}".to_string()
            ]]
    );

    // Every row names a relation, and the catalogs the test itself reads are
    // among them.
    for catalog in ["pg_class", "pg_proc", "pg_init_privs"] {
        assert!(
            scalar(
                &mut session,
                &format!(
                    "SELECT count(*) FROM pg_init_privs p JOIN pg_class c ON c.oid = p.objoid
                     WHERE c.relname = '{catalog}'"
                )
            )
            .await
                == "1",
            "{catalog}"
        );
    }
    // A user relation is not bootstrap state.
    assert!(
        scalar(
            &mut session,
            "SELECT count(*) FROM pg_init_privs p JOIN pg_class c ON c.oid = p.objoid
             WHERE c.relname = 'doc'"
        )
        .await
            == "0"
    );
}

// ------------------------------------------------------- column grants ---

/// The column list binds to its own privilege, and both a stored table and a
/// synthesised catalog take one.
#[tokio::test]
async fn a_column_list_binds_to_the_privilege_it_follows() {
    struct Case {
        name: &'static str,
        setup: &'static str,
        statement: &'static str,
        relation: &'static str,
        expected: &'static [[&'static str; 3]],
    }
    let cases = [
        Case {
            name: "one privilege, one column",
            setup: "CREATE TABLE doc (id int4, body text)",
            statement: "GRANT SELECT (body) ON doc TO reader",
            relation: "doc",
            expected: &[["reader", "body", "SELECT"]],
        },
        Case {
            name: "one privilege, several columns",
            setup: "CREATE TABLE doc (id int4, body text)",
            statement: "GRANT SELECT (id, body) ON doc TO reader",
            relation: "doc",
            expected: &[["reader", "body", "SELECT"], ["reader", "id", "SELECT"]],
        },
        Case {
            name: "a column list per privilege",
            setup: "CREATE TABLE doc (id int4, body text)",
            statement: "GRANT SELECT (id), UPDATE (body) ON doc TO reader",
            relation: "doc",
            expected: &[["reader", "body", "UPDATE"], ["reader", "id", "SELECT"]],
        },
        Case {
            name: "ALL on a column is the four column privileges and not the eight relation ones",
            setup: "CREATE TABLE doc (id int4, body text)",
            statement: "GRANT ALL (body) ON doc TO reader",
            relation: "doc",
            expected: &[
                ["reader", "body", "INSERT"],
                ["reader", "body", "REFERENCES"],
                ["reader", "body", "SELECT"],
                ["reader", "body", "UPDATE"],
            ],
        },
        Case {
            name: "a relation-wide privilege beside a column one writes no column row",
            setup: "CREATE TABLE doc (id int4, body text)",
            statement: "GRANT SELECT, UPDATE (body) ON doc TO reader",
            relation: "doc",
            expected: &[["reader", "body", "UPDATE"]],
        },
        Case {
            name: "a synthesised catalog relation takes one too",
            setup: "SELECT 1",
            statement: "GRANT SELECT (rolname, rolsuper) ON pg_authid TO reader",
            relation: "pg_authid",
            expected: &[
                ["reader", "rolname", "SELECT"],
                ["reader", "rolsuper", "SELECT"],
            ],
        },
    ];

    for case in cases {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run(&mut session, "CREATE ROLE reader").await;
        run(&mut session, case.setup).await;
        run(&mut session, case.statement).await;
        let expected: Vec<Vec<String>> = case
            .expected
            .iter()
            .map(|row| row.iter().map(ToString::to_string).collect())
            .collect();
        assert!(
            column_grants(&mut session, case.relation).await == expected,
            "{}",
            case.name
        );
    }
}

/// A column `REVOKE` takes back exactly what it names.
#[tokio::test]
async fn revoking_a_column_privilege_leaves_the_others() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE ROLE reader;
         CREATE TABLE doc (id int4, body text);
         GRANT SELECT (id, body), UPDATE (body) ON doc TO reader;
         REVOKE SELECT (body) ON doc FROM reader;",
    )
    .await;
    assert!(
        column_grants(&mut session, "doc").await
            == vec![
                vec!["reader".to_string(), "body".into(), "UPDATE".into()],
                vec!["reader".to_string(), "id".into(), "SELECT".into()],
            ]
    );
}

/// `pg_attribute.attacl` carries the grant, which is where `pg_dump` reads it,
/// and stays NULL for every column nobody granted.
#[tokio::test]
async fn a_column_grant_shows_up_in_pg_attribute_attacl() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE ROLE reader;
         CREATE TABLE doc (id int4, body text);
         GRANT SELECT (body) ON doc TO reader;",
    )
    .await;
    let acl = rows_of(
        &mut session,
        "SELECT a.attname, coalesce(a.attacl::text, 'NULL')
         FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
         WHERE c.relname = 'doc' ORDER BY a.attnum",
    )
    .await;
    assert!(
        acl == vec![
            vec!["id".to_string(), "NULL".into()],
            vec!["body".to_string(), "{reader=r/postgres}".into()],
        ]
    );
}

/// A grant follows its column through a rename and dies with it on a drop.
/// Either failure would hand the grant to whatever column takes the name next.
#[tokio::test]
async fn a_column_grant_follows_a_rename_and_dies_with_a_drop() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE ROLE reader;
         CREATE TABLE doc (id int4, body text);
         GRANT SELECT (body) ON doc TO reader;
         ALTER TABLE doc RENAME COLUMN body TO prose;",
    )
    .await;
    assert!(
        column_grants(&mut session, "doc").await
            == vec![vec!["reader".to_string(), "prose".into(), "SELECT".into()]]
    );

    run(&mut session, "ALTER TABLE doc DROP COLUMN prose").await;
    assert!(column_grants(&mut session, "doc").await.is_empty());

    // A fresh column of the old name inherits nothing.
    run(&mut session, "ALTER TABLE doc ADD COLUMN prose text").await;
    assert!(column_grants(&mut session, "doc").await.is_empty());
}

/// The two statements `PostgreSQL` refuses, refused the same way.
#[tokio::test]
async fn a_column_grant_is_refused_where_postgresql_refuses_it() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE ROLE reader; CREATE TABLE doc (id int4, body text);",
    )
    .await;

    for (sql, code, message) in [
        (
            "GRANT SELECT (nosuch) ON doc TO reader",
            "42703",
            "column \"nosuch\" of relation \"doc\" does not exist",
        ),
        (
            "GRANT DELETE (body) ON doc TO reader",
            "0LP01",
            "invalid privilege type DELETE for column",
        ),
        (
            "GRANT TRUNCATE (body) ON doc TO reader",
            "0LP01",
            "invalid privilege type TRUNCATE for column",
        ),
    ] {
        let (actual_code, actual_message) = error_of(&mut session, sql).await;
        assert!(
            (actual_code.as_str(), actual_message.as_str()) == (code, message),
            "{sql}"
        );
    }
    // Nothing was written by any of them.
    assert!(column_grants(&mut session, "doc").await.is_empty());
}

/// A column grant is recorded and does not widen what the grantee may read.
///
/// `PostgreSQL` would admit `SELECT body FROM doc` here and refuse
/// `SELECT id FROM doc`. crabka refuses both, because the read permit is taken
/// for the relation before the projection is known. The narrowing is
/// deliberate and fails closed; this test is what makes widening it a
/// deliberate act rather than a side effect.
#[tokio::test]
async fn a_column_grant_is_recorded_but_does_not_widen_a_read() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(
        &mut owner,
        "CREATE ROLE reader LOGIN;
         CREATE TABLE doc (id int4, body text);
         INSERT INTO doc VALUES (1, 'secret');
         GRANT SELECT (body) ON doc TO reader;",
    )
    .await;

    // Recorded, and only for the column named.
    assert!(
        column_grants(&mut owner, "doc").await
            == vec![vec!["reader".to_string(), "body".into(), "SELECT".into()]]
    );

    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    for sql in [
        "SELECT body FROM doc",
        "SELECT id FROM doc",
        "SELECT * FROM doc",
    ] {
        let (code, message) = error_of(&mut reader, sql).await;
        assert!(
            (code.as_str(), message.as_str()) == ("42501", "permission denied for table doc"),
            "{sql}"
        );
    }

    // The relation-level grant is what admits the read, and it is unaffected by
    // the column grant in either direction.
    run(&mut owner, "GRANT SELECT ON doc TO reader").await;
    assert!(rows_of(&mut reader, "SELECT body FROM doc").await == vec![vec!["secret".to_string()]]);
    run(&mut owner, "REVOKE SELECT (body) ON doc FROM reader").await;
    assert!(rows_of(&mut reader, "SELECT body FROM doc").await == vec![vec!["secret".to_string()]]);
}
