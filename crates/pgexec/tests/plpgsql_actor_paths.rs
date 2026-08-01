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
async fn locking_select_plpgsql_calls_are_statement_atomic() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE lock_source (id int4);
        CREATE TABLE lock_audit (id int4);
        INSERT INTO lock_source VALUES (1), (2);
        CREATE FUNCTION lock_effect(x int4) RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO lock_audit VALUES (x);
          IF x = 2 THEN
            RAISE EXCEPTION 'stop';
          END IF;
          RETURN x * 10;
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("SELECT id, lock_effect(id) FROM lock_source ORDER BY id FOR UPDATE")
        .await
        .expect_err("the second function call fails");
    assert!(error.code == "P0001", "{error:?}");
    assert!(scalar(&mut session, "SELECT count(*) FROM lock_audit").await == Some("0".into()));
}

#[tokio::test]
async fn copy_plpgsql_defaults_are_rejected_before_copy_starts() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION copy_default() RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RETURN 7;
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("CREATE TABLE copy_target (id int4 DEFAULT copy_default())")
        .await
        .expect_err("user routine defaults are unsupported independently of COPY");
    assert!(error.code == "42883", "{error:?}");
}

#[tokio::test]
async fn sharded_timestamp_dml_accepts_pure_plpgsql_calls() {
    let mut engine = SqlEngine::new();
    engine.init_gtm_coordinator().expect("initialize GTM");
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION shard_twice(x int4) RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RETURN x * 2;
        END
        $$;
        CREATE TABLE shard_target (id int4) SHARDED;
        INSERT INTO shard_target VALUES (shard_twice(3))
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT id FROM shard_target").await == Some("6".into()));
}

#[tokio::test]
async fn sharded_timestamp_dml_rejects_session_effects_atomically() {
    let mut engine = SqlEngine::new();
    engine.init_gtm_coordinator().expect("initialize GTM");
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE shard_audit (id int4);
        CREATE TABLE shard_effect_target (id int4) SHARDED;
        CREATE FUNCTION shard_effect(x int4) RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO shard_audit VALUES (x);
          RETURN x;
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("INSERT INTO shard_effect_target VALUES (shard_effect(7))")
        .await
        .expect_err("mixed local and timestamp side effects are unsupported");
    assert!(error.code == "0A000", "{error:?}");
    assert!(
        scalar(&mut session, "SELECT count(*) FROM shard_effect_target").await == Some("0".into())
    );
    assert!(scalar(&mut session, "SELECT count(*) FROM shard_audit").await == Some("0".into()));
}
