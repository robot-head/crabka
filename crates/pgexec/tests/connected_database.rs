//! The database a session connected to, as every projection of it reports it.
//!
//! The engine used to hold the name as one constant, `"postgres"`, and answer
//! with it whatever the client had asked for. That made
//! `SELECT dathasloginevt FROM pg_database WHERE datname = :'DBNAME'` — the
//! last statement of the upstream `event_trigger_login` test — return no rows
//! at all on a connection to any other name.
//!
//! A constant also decided questions it had no business deciding. A three-part
//! name is local when its catalog part is *this* database, and `REINDEX
//! DATABASE` accepts the open database and nothing else. Measured against the
//! constant, `postgres.public.t` resolved locally from every database while the
//! connected database's own name read as somebody else's, and `REINDEX
//! DATABASE <the database you are in>` was refused while `REINDEX DATABASE
//! postgres` succeeded from anywhere. Both are pinned below.
//!
//! The engine serves one database per process and does not police the name, so
//! the name is the connection's rather than the server's. That is the property
//! these tests state: what the session asked for is what every projection
//! says, and two sessions that asked for different names each get their own
//! answer.

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

/// Every cell of the one row a query returns, or `None` when it returned none.
async fn row(session: &mut SqlSession, sql: &str) -> Option<Vec<String>> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => match rows.as_slice() {
            [] => None,
            [only] => Some(only.iter().map(|cell| cell_text(cell.as_ref())).collect()),
            many => panic!("expected at most one row from {sql}, got {many:?}"),
        },
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    let cells = row(session, sql)
        .await
        .unwrap_or_else(|| panic!("expected a row from {sql}"));
    let [cell] = cells.as_slice() else {
        panic!("expected one column from {sql}, got {cells:?}");
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

/// A session that connected to `database`, the way the wire layer opens one.
fn session_on(engine: &SqlEngine, database: &str) -> SqlSession {
    let mut session = engine.connect();
    session.set_database(database);
    session
}

/// Every projection of the session's own database agrees with the startup
/// packet, for any name a client may connect with.
#[tokio::test]
async fn every_projection_names_the_database_the_session_connected_to() {
    // `crab` is what the pg_regress harness connects as, `regression` what
    // upstream's own runs use, and `postgres` the engine's default — which has
    // to keep working and must not be the only one that does.
    for database in ["crab", "regression", "postgres", "Mixed Case"] {
        let engine = SqlEngine::new();
        let mut session = session_on(&engine, database);
        run(&mut session, "CREATE TABLE doc (id int4)").await;

        let queries = [
            "SELECT current_database()",
            "SELECT datname FROM pg_database",
            "SELECT datname FROM pg_stat_activity",
            "SELECT catalog_name FROM information_schema.schemata WHERE schema_name = 'public'",
            "SELECT table_catalog FROM information_schema.tables WHERE table_name = 'doc'",
        ];
        for query in queries {
            assert!(
                scalar(&mut session, query).await == database,
                "{query} on {database}"
            );
        }
    }
}

/// The `event_trigger_login` statement itself: the row is found by the name the
/// client connected with, and its `dathasloginevt` tracks the trigger.
#[tokio::test]
async fn pg_database_is_found_by_the_connected_name_and_reports_a_login_trigger() {
    let engine = SqlEngine::new();
    let mut session = session_on(&engine, "regression");
    let query = "SELECT dathasloginevt FROM pg_database WHERE datname = 'regression'";

    assert!(scalar(&mut session, query).await == "f");

    run(
        &mut session,
        "CREATE FUNCTION on_login_proc() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'You are welcome!';
         END;
         $$ LANGUAGE plpgsql;
         CREATE EVENT TRIGGER on_login_trigger ON login
           EXECUTE PROCEDURE on_login_proc();",
    )
    .await;
    assert!(scalar(&mut session, query).await == "t");

    // The flag is computed, not latched: dropping the trigger clears it, the
    // way `PostgreSQL` clears it once no login trigger is left.
    run(&mut session, "DROP EVENT TRIGGER on_login_trigger").await;
    assert!(scalar(&mut session, query).await == "f");

    // A DDL event trigger is not a login one and must not raise the flag.
    run(
        &mut session,
        "CREATE EVENT TRIGGER on_ddl ON ddl_command_end
           EXECUTE PROCEDURE on_login_proc();",
    )
    .await;
    assert!(scalar(&mut session, query).await == "f");
}

/// A session sees its own database and not the name another session used, and
/// no session sees a row under a name nobody connected with.
#[tokio::test]
async fn two_sessions_on_one_engine_each_report_their_own_database() {
    let engine = SqlEngine::new();
    let mut sales = session_on(&engine, "sales");
    let mut audit = session_on(&engine, "audit");

    assert!(scalar(&mut sales, "SELECT current_database()").await == "sales");
    assert!(scalar(&mut audit, "SELECT current_database()").await == "audit");
    assert!(
        row(
            &mut sales,
            "SELECT datname FROM pg_database WHERE datname = 'audit'"
        )
        .await
            == None
    );
    assert!(
        row(
            &mut audit,
            "SELECT datname FROM pg_database WHERE datname = 'postgres'"
        )
        .await
            == None
    );
}

/// A three-part relation name is local when its catalog part is this database,
/// and a cross-database reference otherwise — measured against the session and
/// not against a constant.
#[tokio::test]
async fn a_three_part_name_is_local_only_for_this_session_s_database() {
    let engine = SqlEngine::new();
    let mut session = session_on(&engine, "sales");
    run(&mut session, "CREATE TABLE doc (id int4)").await;

    assert!(scalar(&mut session, "SELECT 'sales.public.doc'::regclass::text").await == "doc");

    // The old constant made this one succeed from every database.
    let (code, message) = error_of(&mut session, "SELECT 'postgres.public.doc'::regclass").await;
    assert!(
        (code.as_str(), message.as_str())
            == ("0A000", {
                "cross-database references are not implemented: \"postgres.public.doc\""
            })
    );

    // `regtype` reads a three-part name through the same rule.
    assert!(
        scalar(
            &mut session,
            "SELECT 'sales.pg_catalog.int4'::regtype::text"
        )
        .await
            == "integer"
    );
    let (code, _) = error_of(&mut session, "SELECT 'postgres.pg_catalog.int4'::regtype").await;
    assert!(code == "0A000");
}

/// `REINDEX DATABASE` names the open database and nothing else. Against a
/// constant the rule ran backwards: the connected database was refused and
/// `postgres` was accepted from anywhere.
#[tokio::test]
async fn reindex_database_accepts_the_open_database_and_refuses_the_rest() {
    let engine = SqlEngine::new();
    let mut session = session_on(&engine, "sales");

    run(&mut session, "REINDEX DATABASE sales").await;
    run(&mut session, "REINDEX SYSTEM sales").await;

    for sql in ["REINDEX DATABASE postgres", "REINDEX SYSTEM postgres"] {
        let (code, message) = error_of(&mut session, sql).await;
        assert!(
            (code.as_str(), message.as_str())
                == ("0A000", "can only reindex the currently open database"),
            "{sql}"
        );
    }
}

/// A session nothing told a database to keeps the engine's default, so an
/// embedded caller and a unit test still read like a fresh cluster.
#[tokio::test]
async fn a_session_with_no_startup_packet_keeps_the_default_database() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    assert!(scalar(&mut session, "SELECT current_database()").await == "postgres");
    assert!(scalar(&mut session, "SELECT datname FROM pg_database").await == "postgres");
}
