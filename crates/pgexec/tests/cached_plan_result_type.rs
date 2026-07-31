//! A statement prepared under one `search_path` and run under another is
//! re-analysed against the path in force when it runs — `postgres:18.4` does
//! the same, so an unqualified name really can reach a different relation than
//! the one described to the client. What 18.4 will not do is answer with a
//! result of a shape it has already announced a different one for: it raises
//! `0A000 cached plan must not change result type`.
//!
//! Every expectation here was taken from a live `postgres:18.4`, over a
//! protocol-level Parse/Bind/Execute (named and unnamed alike) and a SQL-level
//! PREPARE/EXECUTE, which answer identically.
//!
//! The check is what keeps the wire honest. `Bind` freezes the result formats
//! at the field count the descriptor announced and `Execute` zips row cells
//! against them, so without it a wider relation is silently truncated to the
//! announced count and a narrower one puts fewer fields on the wire than its
//! own `RowDescription` promised, carrying the earlier relation's type oids.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, ExecuteOutcome, QueryResult, Session};

/// What running the prepared statement under the second search path produced:
/// the column names it answered with, or the SQLSTATE and message it was
/// refused with.
type Outcome = Result<Vec<String>, (String, String)>;

fn refused() -> Outcome {
    Err((
        "0A000".to_string(),
        "cached plan must not change result type".to_string(),
    ))
}

fn answered(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|name| (*name).to_string()).collect()
}

/// One case: how `t` is declared in each schema, and what the second run does.
struct Case {
    what_changed: &'static str,
    in_first_schema: &'static str,
    in_second_schema: &'static str,
    outcome: Outcome,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            what_changed: "a wider result",
            in_first_schema: "a int",
            in_second_schema: "a int, b int",
            outcome: refused(),
        },
        Case {
            what_changed: "a narrower result",
            in_first_schema: "a int, b int",
            in_second_schema: "a int",
            outcome: refused(),
        },
        Case {
            what_changed: "a retyped column",
            in_first_schema: "a int",
            in_second_schema: "a text",
            outcome: refused(),
        },
        Case {
            what_changed: "a renamed column",
            in_first_schema: "a int",
            in_second_schema: "z int",
            outcome: refused(),
        },
        Case {
            what_changed: "nothing but the relation behind the name",
            in_first_schema: "a int",
            in_second_schema: "a int",
            outcome: Ok(answered(&["a"])),
        },
    ]
}

async fn run(session: &mut SqlSession, sql: &str) {
    session.simple_query(sql).await.expect("statement succeeds");
}

async fn two_schemas(session: &mut SqlSession, case: &Case) {
    run(session, "CREATE SCHEMA s1").await;
    run(session, "CREATE SCHEMA s2").await;
    run(
        session,
        &format!("CREATE TABLE s1.t ({})", case.in_first_schema),
    )
    .await;
    run(
        session,
        &format!("CREATE TABLE s2.t ({})", case.in_second_schema),
    )
    .await;
}

fn refusal(error: &crabka_pgwire::error::PgError) -> (String, String) {
    (error.code.clone(), error.message.clone())
}

/// Parse under `s1`, then Bind and Execute under `s2`.
async fn over_the_protocol(case: &Case, statement: &str) -> Outcome {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    two_schemas(&mut session, case).await;

    run(&mut session, "SET search_path = s1").await;
    session
        .parse(statement, "SELECT * FROM t", &[])
        .await
        .expect("parse succeeds under the first path");
    run(&mut session, "SET search_path = s2").await;

    let described = match session.bind("p", statement, &[], &[]).await {
        Ok(description) => description,
        Err(error) => return Err(refusal(&error)),
    };
    match session.execute("p", 0).await {
        Ok(ExecuteOutcome::Rows { .. } | ExecuteOutcome::EmptyQuery) => Ok(described
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect()),
        Ok(other) => panic!("expected rows, got {other:?}"),
        Err(error) => Err(refusal(&error)),
    }
}

/// `PREPARE` under `s1`, `EXECUTE` under `s2`.
async fn over_sql_prepare(case: &Case) -> Outcome {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    two_schemas(&mut session, case).await;

    run(&mut session, "SET search_path = s1").await;
    run(&mut session, "PREPARE p AS SELECT * FROM t").await;
    run(&mut session, "SET search_path = s2").await;

    match session.simple_query("EXECUTE p").await {
        Ok(results) => match results.into_iter().next_back().expect("one result") {
            QueryResult::Rows { fields, .. } => {
                Ok(fields.iter().map(|field| field.name.clone()).collect())
            }
            other => panic!("expected rows, got {other:?}"),
        },
        Err(error) => Err(refusal(&error)),
    }
}

#[tokio::test]
async fn a_protocol_prepared_statement_may_not_change_its_result_type() {
    for case in cases() {
        assert!(
            over_the_protocol(&case, "named").await == case.outcome,
            "{}",
            case.what_changed
        );
    }
}

/// `postgres:18.4` makes no exception for the unnamed statement: an unnamed
/// Parse whose result shape moved before its Bind is refused exactly as a named
/// one is.
#[tokio::test]
async fn an_unnamed_prepared_statement_may_not_change_its_result_type() {
    for case in cases() {
        assert!(
            over_the_protocol(&case, "").await == case.outcome,
            "{}",
            case.what_changed
        );
    }
}

#[tokio::test]
async fn a_sql_prepared_statement_may_not_change_its_result_type() {
    for case in cases() {
        assert!(
            over_sql_prepare(&case).await == case.outcome,
            "{}",
            case.what_changed
        );
    }
}

/// The search path is not the only way a cached statement's result can move:
/// `postgres:18.4` refuses the same way when the relation itself is altered
/// under a statement whose path never changed.
#[tokio::test]
async fn a_relation_altered_under_a_prepared_statement_is_refused_the_same_way() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a int)").await;
    run(&mut session, "PREPARE p AS SELECT * FROM t").await;
    run(&mut session, "EXECUTE p").await;

    run(&mut session, "ALTER TABLE t ADD COLUMN b int").await;

    let error = session
        .simple_query("EXECUTE p")
        .await
        .expect_err("the widened result is refused");
    assert!(
        refusal(&error)
            == (
                "0A000".to_string(),
                "cached plan must not change result type".to_string()
            )
    );
}

/// The same statement, prepared and run under one unchanged path, is never
/// re-described and never refused — including when it is run more than once.
#[tokio::test]
async fn an_unchanged_search_path_leaves_a_prepared_statement_alone() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a int, b text)").await;
    run(&mut session, "INSERT INTO t VALUES (1, 'x')").await;

    session
        .parse("p", "SELECT * FROM t", &[])
        .await
        .expect("parse");
    for portal in ["first", "second"] {
        let described = session.bind(portal, "p", &[], &[]).await.expect("bind");
        assert!(
            described
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
                == vec!["a".to_string(), "b".to_string()]
        );
        let ExecuteOutcome::Rows { rows, .. } = session.execute(portal, 0).await.expect("execute")
        else {
            panic!("expected rows");
        };
        assert!(rows.len() == 1);
        assert!(rows[0].len() == 2);
    }
}
