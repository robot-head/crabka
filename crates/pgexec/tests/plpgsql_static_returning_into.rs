use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn execute(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error:?}"))
}

async fn scalar(session: &mut SqlSession, sql: &str) -> Option<String> {
    let results = execute(session, sql).await;
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("`{sql}` did not return rows: {:?}", results[0]);
    };
    assert!(
        rows.len() == 1 && rows[0].len() == 1,
        "`{sql}` returned {rows:?}"
    );
    rows[0][0].as_ref().map(cell_text)
}

fn cell_text(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("server text is UTF-8")
}

#[tokio::test]
async fn static_insert_returning_into_assigns_one_row() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE static_returning_input (n int4);
        CREATE TABLE static_returning_result (n int4);
        DO $$
        DECLARE picked int4;
        BEGIN
          INSERT INTO static_returning_input VALUES (7) RETURNING n INTO picked;
          INSERT INTO static_returning_result VALUES (picked);
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT n FROM static_returning_result").await == Some("7".into())
    );
}

#[tokio::test]
async fn static_update_returning_into_assigns_null_for_zero_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE static_returning_input (n int4);
        CREATE TABLE static_returning_result (n int4);
        INSERT INTO static_returning_input VALUES (1);
        DO $$
        DECLARE picked int4 := 99;
        BEGIN
          UPDATE static_returning_input SET n = n + 1 WHERE n = 2 RETURNING n INTO picked;
          INSERT INTO static_returning_result VALUES (picked);
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT n FROM static_returning_result")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn static_delete_returning_into_rejects_multiple_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE static_returning_input (n int4);
        CREATE TABLE static_returning_result (state text);
        INSERT INTO static_returning_input VALUES (1), (2);
        DO $$
        DECLARE picked int4;
        BEGIN
          BEGIN
            DELETE FROM static_returning_input RETURNING n INTO picked;
          EXCEPTION WHEN too_many_rows THEN
            INSERT INTO static_returning_result VALUES (SQLSTATE);
          END;
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT state FROM static_returning_result").await
            == Some("P0003".into())
    );
    assert!(
        scalar(&mut session, "SELECT count(*) FROM static_returning_input").await
            == Some("2".into())
    );
}
