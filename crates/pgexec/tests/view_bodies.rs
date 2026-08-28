//! What a `CREATE VIEW` body may be, and what the engine knows about it
//! afterwards.
//!
//! A view is stored as its SQL text and handed to the ordinary query executor
//! on every scan, so the set of bodies it can hold is the set of queries that
//! executor can run: joins, derived tables, subqueries, set operations, `WITH`,
//! and views over views. What is refused is only what `PostgreSQL` refuses
//! (a query parameter, a data-modifying `WITH` entry) plus a locking clause,
//! which this engine's read path cannot honour.
//!
//! The other half is what the catalog then knows: every relation the body
//! reads, however deeply the shape buries it, because that is what decides
//! where a view over a temporary relation lands and what `DROP TABLE` may
//! remove. A `WITH` name is deliberately *not* one of them.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
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

async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match run(session, sql).await.pop().expect("one result") {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// Three small tables, enough for a join, a subquery and a set operation.
async fn seeded() -> (SqlEngine, Arc<dyn Kv>) {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (a int, b text, c int);
         INSERT INTO t VALUES (1,'x',10),(2,'y',20),(3,'z',30);
         CREATE TABLE u (a int, d text);
         INSERT INTO u VALUES (1,'p'),(2,'q'),(4,'r');
         CREATE TABLE w (a int, e int);
         INSERT INTO w VALUES (1,100),(3,300);",
    )
    .await;
    (engine, kv)
}

/// Each case is a view body and the rows reading that view answers, ordered.
/// The shapes are the ones the validator used to refuse outright.
#[tokio::test]
async fn a_view_body_may_be_any_query_the_executor_runs() {
    let cases: [(&str, &[&str]); 20] = [
        (
            "SELECT t.a, u.d FROM t JOIN u ON t.a = u.a",
            &["1,p", "2,q"],
        ),
        (
            "SELECT t.a, u.d FROM t LEFT JOIN u ON t.a = u.a",
            &["1,p", "2,q", "3,NULL"],
        ),
        (
            "SELECT a, b, d FROM t JOIN u USING (a)",
            &["1,x,p", "2,y,q"],
        ),
        ("SELECT t.a, u.d FROM t, u WHERE t.a = u.a", &["1,p", "2,q"]),
        (
            "SELECT t.a, u.d, w.e FROM t JOIN u ON t.a = u.a JOIN w ON w.a = t.a",
            &["1,p,100"],
        ),
        (
            "SELECT s.a, s.b FROM (SELECT a, b FROM t WHERE a > 1) s",
            &["2,y", "3,z"],
        ),
        (
            "SELECT t.a, l.d FROM t, LATERAL (SELECT d FROM u WHERE u.a = t.a) l",
            &["1,p", "2,q"],
        ),
        (
            "SELECT a, (SELECT max(e) FROM w) AS m FROM t",
            &["1,300", "2,300", "3,300"],
        ),
        (
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
            &["1", "2"],
        ),
        ("SELECT a FROM t WHERE a IN (SELECT a FROM u)", &["1", "2"]),
        (
            "SELECT a FROM t WHERE a = ANY (SELECT a FROM w)",
            &["1", "3"],
        ),
        (
            "SELECT a FROM t UNION SELECT a FROM u",
            &["1", "2", "3", "4"],
        ),
        ("SELECT a FROM t INTERSECT SELECT a FROM u", &["1", "2"]),
        ("SELECT a FROM t EXCEPT SELECT a FROM u", &["3"]),
        ("SELECT * FROM (VALUES (1),(2)) AS v(a)", &["1", "2"]),
        (
            "WITH s AS (SELECT a FROM t WHERE a > 1) SELECT a FROM s",
            &["2", "3"],
        ),
        (
            "WITH s AS (SELECT a FROM t), r AS (SELECT a FROM u) \
             SELECT s.a FROM s JOIN r ON s.a = r.a",
            &["1", "2"],
        ),
        (
            "WITH RECURSIVE n(a) AS (SELECT 1 UNION ALL SELECT a+1 FROM n WHERE a < 3) \
             SELECT a FROM n",
            &["1", "2", "3"],
        ),
        (
            "SELECT b, count(*) AS n FROM t GROUP BY b HAVING count(*) > (SELECT 0)",
            &["x,1", "y,1", "z,1"],
        ),
        (
            "SELECT a, sum(c) OVER (ORDER BY a) AS running FROM t",
            &["1,10", "2,30", "3,60"],
        ),
    ];
    for (body, expected) in cases {
        let (engine, _kv) = seeded().await;
        let mut session = engine.connect();
        run(&mut session, &format!("CREATE VIEW v AS {body}")).await;
        assert!(
            query(&mut session, "SELECT * FROM v ORDER BY 1").await == rows(expected),
            "{body}"
        );
    }
}

/// A view over a view, and a view joining one — each level expanded through the
/// same read path, so the outer body sees the inner view's output columns.
#[tokio::test]
async fn a_view_may_read_another_view() {
    let (engine, _kv) = seeded().await;
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE VIEW v1 AS SELECT a, b FROM t;
         CREATE VIEW v2 AS SELECT a FROM v1 WHERE a > 1;
         CREATE VIEW v3 AS SELECT v2.a, u.d FROM v2 JOIN u ON v2.a = u.a;
         CREATE VIEW v4 AS SELECT a FROM v1 UNION ALL SELECT a FROM v2;",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM v2 ORDER BY 1").await == rows(&["2", "3"]));
    assert!(query(&mut session, "SELECT * FROM v3 ORDER BY 1").await == rows(&["2,q"]));
    assert!(
        query(&mut session, "SELECT * FROM v4 ORDER BY 1").await
            == rows(&["1", "2", "2", "3", "3"])
    );
}

#[tokio::test]
async fn a_view_may_call_the_interpt_pp_regression_c_adapter() {
    let (engine, _kv) = seeded().await;
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE FUNCTION interpt_pp(path, path) RETURNS point AS 'regress' LANGUAGE C STRICT;
         CREATE VIEW crossing AS
         SELECT interpt_pp('[(0,0),(2,2)]'::path, '[(0,2),(2,0)]'::path) AS point;",
    )
    .await;
    assert!(query(&mut session, "SELECT point FROM crossing").await == rows(&["(1,1)"]));
}

/// The only bodies still refused, with the SQLSTATE each answers.
#[tokio::test]
async fn the_bodies_that_are_still_refused() {
    let cases = [
        (
            "CREATE VIEW v AS SELECT a FROM t FOR UPDATE",
            "0A000",
            "CREATE VIEW does not support locking SELECT",
        ),
        (
            "CREATE VIEW v AS SELECT a FROM t WHERE a = $1",
            "42P02",
            "there is no parameter $1",
        ),
        // The walk reaches a parameter through a subquery too, which the old
        // per-clause check could not do because it refused the subquery first.
        (
            "CREATE VIEW v AS SELECT a FROM t WHERE a IN (SELECT a FROM u WHERE a = $2)",
            "42P02",
            "there is no parameter $2",
        ),
        (
            "CREATE VIEW v AS SELECT s.a FROM (SELECT a FROM u WHERE a = $3) s",
            "42P02",
            "there is no parameter $3",
        ),
        (
            "CREATE VIEW v AS WITH d AS (DELETE FROM t RETURNING a) SELECT a FROM d",
            "0A000",
            "views must not contain data-modifying statements in WITH",
        ),
    ];
    for (sql, sqlstate, message) in cases {
        let (engine, _kv) = seeded().await;
        let mut session = engine.connect();
        assert!(
            error_of(&mut session, sql).await == (sqlstate.to_string(), message.to_string()),
            "{sql}"
        );
    }
}

/// A relation the body names but the catalog does not hold is reported as the
/// missing relation, wherever in the shape it sits — not as a refusal of the
/// shape itself.
#[tokio::test]
async fn a_missing_relation_is_reported_as_missing() {
    let cases = [
        "CREATE VIEW v AS SELECT a FROM t JOIN nope n ON n.a = t.a",
        "CREATE VIEW v AS SELECT (SELECT z FROM nope) FROM t",
        "CREATE VIEW v AS SELECT s.z FROM (SELECT z FROM nope) s",
        "CREATE VIEW v AS SELECT a FROM t UNION ALL SELECT z FROM nope",
        "CREATE VIEW v AS WITH s AS (SELECT z FROM nope) SELECT z FROM s",
    ];
    for sql in cases {
        let (engine, _kv) = seeded().await;
        let mut session = engine.connect();
        assert!(
            error_of(&mut session, sql).await
                == (
                    "42P01".to_string(),
                    "relation \"nope\" does not exist".to_string()
                ),
            "{sql}"
        );
    }
}

/// `DROP TABLE` sees the relation however deeply the body buries it. Each case
/// is a view body reading `dep` through one shape; dropping `dep` must be
/// refused, and must succeed once the view is gone.
#[tokio::test]
async fn every_relation_a_body_reads_blocks_dropping_it() {
    let cases = [
        "SELECT t.a, dep.z FROM t JOIN dep ON dep.z = t.a",
        "SELECT s.z FROM (SELECT z FROM dep) s",
        "SELECT a, (SELECT max(z) FROM dep) AS m FROM t",
        "SELECT a FROM t WHERE a IN (SELECT z FROM dep)",
        "SELECT a FROM t UNION ALL SELECT z FROM dep",
        "WITH s AS (SELECT z FROM dep) SELECT z FROM s",
        "SELECT a FROM t ORDER BY (SELECT max(z) FROM dep)",
        "SELECT t.a FROM t, LATERAL (SELECT z FROM dep WHERE z = t.a) l",
    ];
    for body in cases {
        let (engine, _kv) = seeded().await;
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE dep (z int)").await;
        run(&mut session, &format!("CREATE VIEW v AS {body}")).await;
        let (sqlstate, message) = error_of(&mut session, "DROP TABLE dep").await;
        assert!(sqlstate == "2BP01", "{body}");
        assert!(
            message == "cannot drop table dep because other objects depend on it",
            "{body}"
        );
        run(&mut session, "DROP VIEW v").await;
        run(&mut session, "DROP TABLE dep").await;
    }
}

/// A `WITH` name is a query, not a relation, so a body whose `FROM` resolves to
/// its own entry does not depend on the relation of that name — and dropping
/// that relation leaves the view working.
#[tokio::test]
async fn a_cte_name_is_not_a_dependency() {
    let (engine, _kv) = seeded().await;
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE shadowed (z int);
         INSERT INTO shadowed VALUES (99);
         CREATE VIEW v AS WITH shadowed AS (SELECT 7 AS z) SELECT z FROM shadowed;",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM v").await == rows(&["7"]));
    run(&mut session, "DROP TABLE shadowed").await;
    assert!(query(&mut session, "SELECT * FROM v").await == rows(&["7"]));
}

/// Where a view lands is decided by everything its body reads, so a temporary
/// relation reached through a join arm or a subquery makes the view temporary
/// just as one in a lone `FROM` does.
#[tokio::test]
async fn a_temporary_relation_anywhere_in_the_body_makes_the_view_temporary() {
    let cases = [
        "SELECT x FROM tmp",
        "SELECT t.a FROM t JOIN tmp ON tmp.x = t.a",
        "SELECT a FROM t WHERE a IN (SELECT x FROM tmp)",
        "SELECT s.x FROM (SELECT x FROM tmp) s",
        "SELECT a FROM t UNION ALL SELECT x FROM tmp",
        "WITH s AS (SELECT x FROM tmp) SELECT x FROM s",
    ];
    for body in cases {
        let (engine, _kv) = seeded().await;
        let mut session = engine.connect();
        run(&mut session, "CREATE TEMP TABLE tmp (x int)").await;
        run(&mut session, &format!("CREATE VIEW v AS {body}")).await;
        let schema = query(
            &mut session,
            "SELECT relnamespace::regnamespace::text FROM pg_class WHERE relname = 'v'",
        )
        .await;
        assert!(
            schema
                .first()
                .is_some_and(|name| name.starts_with("pg_temp")),
            "{body} landed in {schema:?}"
        );
        // Naming an ordinary schema for such a view is PostgreSQL's 42P16.
        let mut other = engine.connect();
        run(&mut other, "CREATE TEMP TABLE tmp (x int)").await;
        let (sqlstate, message) =
            error_of(&mut other, &format!("CREATE VIEW public.v2 AS {body}")).await;
        assert!(sqlstate == "42P16", "{body}");
        assert!(
            message == "cannot create temporary relation in non-temporary schema",
            "{body}"
        );
    }
}

/// `CREATE OR REPLACE VIEW` keeps its rules once the body may change shape:
/// the existing columns keep their names and types, and only trailing ones may
/// be added.
#[tokio::test]
async fn or_replace_still_only_appends_columns() {
    let (engine, _kv) = seeded().await;
    let mut session = engine.connect();
    run(&mut session, "CREATE VIEW v AS SELECT a, b FROM t").await;
    // Widening the shape while keeping the columns is allowed.
    run(
        &mut session,
        "CREATE OR REPLACE VIEW v AS SELECT t.a, t.b FROM t JOIN u ON t.a = u.a",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM v ORDER BY 1").await == rows(&["1,x", "2,y"]));
    // So is appending a column while widening.
    run(
        &mut session,
        "CREATE OR REPLACE VIEW v AS SELECT t.a, t.b, u.d FROM t JOIN u ON t.a = u.a",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM v ORDER BY 1").await == rows(&["1,x,p", "2,y,q"]));
    let cases = [
        (
            "CREATE OR REPLACE VIEW v AS SELECT a, b FROM t",
            "cannot drop columns from view",
        ),
        (
            "CREATE OR REPLACE VIEW v AS SELECT a AS z, b, c FROM t",
            "cannot change name of view column \"a\" to \"z\"",
        ),
        (
            "CREATE OR REPLACE VIEW v AS SELECT a::text AS a, b, c FROM t",
            "cannot change data type of view column \"a\" from integer to text",
        ),
        (
            "CREATE OR REPLACE VIEW v AS SELECT a, b, c FROM t UNION ALL SELECT a, b, c FROM t",
            "cannot change name of view column \"d\" to \"c\"",
        ),
    ];
    for (sql, message) in cases {
        assert!(
            error_of(&mut session, sql).await == ("42P16".to_string(), message.to_string()),
            "{sql}"
        );
    }
}

/// The view's columns are named from its select list, and an expression with no
/// name of its own is `?column?` — the same labelling any other query gets.
#[tokio::test]
async fn output_columns_are_named_from_the_select_list() {
    let cases: [(&str, &[&str]); 8] = [
        ("SELECT a + 1 FROM t", &["?column?"]),
        ("SELECT a + 1 AS plus FROM t", &["plus"]),
        ("SELECT count(*) FROM t", &["count"]),
        ("SELECT t.a, u.d FROM t JOIN u ON t.a = u.a", &["a", "d"]),
        ("SELECT s.a FROM (SELECT a FROM t) s", &["a"]),
        // A set operation is named by its left arm.
        (
            "SELECT a AS ll FROM t UNION ALL SELECT a AS rr FROM u",
            &["ll"],
        ),
        ("SELECT * FROM (VALUES (1,'x')) AS v(p, q)", &["p", "q"]),
        (
            "WITH s AS (SELECT a AS inner_name FROM t) SELECT inner_name FROM s",
            &["inner_name"],
        ),
    ];
    for (body, expected) in cases {
        let (engine, _kv) = seeded().await;
        let mut session = engine.connect();
        run(&mut session, &format!("CREATE VIEW v AS {body}")).await;
        let names = query(
            &mut session,
            "SELECT attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
              WHERE c.relname = 'v' AND a.attnum > 0 ORDER BY a.attnum",
        )
        .await;
        assert!(names == rows(expected), "{body}");
    }
}
