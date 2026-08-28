//! `ALTER TABLE … REPLICA IDENTITY` catalog state and index eligibility.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8"))
}

async fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|cell| text(cell.as_ref())).collect())
            .collect(),
        result => panic!("expected rows, got {result:?}"),
    }
}

async fn error(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session.simple_query(sql).await.expect_err("expected error");
    (error.code, error.message)
}

fn row(values: &[&str]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| Some((*value).to_string()))
        .collect()
}

#[tokio::test]
async fn replica_identity_modes_are_visible_in_pg_class_and_pg_index() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (id int NOT NULL, code int NOT NULL); \
         CREATE UNIQUE INDEX t_id ON t (id); \
         CREATE UNIQUE INDEX t_code ON t (code)",
    )
    .await;
    let indexes = "SELECT c.relreplident, ci.relname, i.indisreplident \
                   FROM pg_class c JOIN pg_index i ON i.indrelid = c.oid \
                   JOIN pg_class ci ON ci.oid = i.indexrelid \
                   WHERE c.relname = 't' ORDER BY ci.relname";

    run(
        &mut session,
        "ALTER TABLE t REPLICA IDENTITY USING INDEX t_id",
    )
    .await;
    assert!(
        rows(&mut session, indexes).await
            == vec![row(&["i", "t_code", "f"]), row(&["i", "t_id", "t"])]
    );

    for (sql, mode) in [
        ("ALTER TABLE t REPLICA IDENTITY FULL", "f"),
        ("ALTER TABLE t REPLICA IDENTITY NOTHING", "n"),
        ("ALTER TABLE t REPLICA IDENTITY DEFAULT", "d"),
    ] {
        run(&mut session, sql).await;
        assert!(
            rows(&mut session, indexes).await
                == vec![row(&[mode, "t_code", "f"]), row(&[mode, "t_id", "f"])],
            "{sql}"
        );
    }
}

#[tokio::test]
async fn replica_identity_requires_a_unique_immediate_not_null_index() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (id int NOT NULL, optional int); \
         CREATE INDEX t_id_plain ON t (id); \
         CREATE UNIQUE INDEX t_optional ON t (optional); \
         CREATE UNIQUE INDEX t_id ON t (id)",
    )
    .await;

    assert!(
        error(
            &mut session,
            "ALTER TABLE t REPLICA IDENTITY USING INDEX t_id_plain"
        )
        .await
            == (
                "42809".into(),
                "cannot use non-unique index \"t_id_plain\" as replica identity".into()
            )
    );
    assert!(
        error(
            &mut session,
            "ALTER TABLE t REPLICA IDENTITY USING INDEX t_optional"
        )
        .await
            == (
                "42809".into(),
                "index \"t_optional\" cannot be used as replica identity because column \"optional\" is nullable".into()
            )
    );

    run(
        &mut session,
        "ALTER TABLE t REPLICA IDENTITY USING INDEX t_id",
    )
    .await;
    assert!(
        error(&mut session, "ALTER TABLE t ALTER COLUMN id DROP NOT NULL").await
            == (
                "42P16".into(),
                "column \"id\" is in index used as replica identity".into()
            )
    );
}
