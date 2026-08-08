//! Schema-qualified relation names against a real in-process engine.
//!
//! Every statement that names a relation now goes through one parse function
//! that produces a `RelationRef`, so a dotted name means the same thing
//! everywhere. Before that, `SELECT * FROM s1.t` and `INSERT INTO s1.t` failed
//! with different SQLSTATEs in different phases.
//!
//! Which SQLSTATE a missing schema draws was captured from a live
//! `PostgreSQL` 18.4 rather than read from documentation, because the split is
//! not the obvious one. DML reports the *relation*, with `42P01`, while every
//! utility statement reports the *schema*, with `3F000`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("statement should succeed")
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

fn rows_text(r: &QueryResult) -> Vec<Vec<Option<String>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// A failed statement as `(SQLSTATE, message)`, compared as one value so a case
/// states its whole expected error.
#[derive(Debug, PartialEq, Eq)]
struct Failure(String, String);

async fn failure_of(s: &mut SqlSession, sql: &str) -> Failure {
    let error = s.simple_query(sql).await.expect_err("expected an error");
    Failure(error.code, error.message)
}

fn schema_missing(schema: &str) -> Failure {
    Failure(
        "3F000".into(),
        format!("schema \"{schema}\" does not exist"),
    )
}

fn relation_missing(relation: &str) -> Failure {
    Failure(
        "42P01".into(),
        format!("relation \"{relation}\" does not exist"),
    )
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

/// A `SELECT`/DML target reports the missing *relation*, naming the dotted,
/// case-folded form, and never the missing schema. Captured from 18.4.
#[tokio::test]
async fn a_dml_target_in_a_missing_schema_is_an_undefined_relation() {
    let (_engine, mut s) = engine_with(&[]).await;
    for sql in [
        "SELECT * FROM nope.t",
        "SELECT * FROM nope.t AS x",
        "INSERT INTO nope.t VALUES (1)",
        "UPDATE nope.t SET x = 1",
        "DELETE FROM nope.t",
        "MERGE INTO nope.t USING (SELECT 1 AS a) src ON true WHEN MATCHED THEN DO NOTHING",
    ] {
        assert!(
            failure_of(&mut s, sql).await == relation_missing("nope.t"),
            "{sql}"
        );
    }
}

/// Every utility statement resolves the schema first, so the schema is what it
/// reports. This is the half of the split the parser used to make for it, and
/// the reason the decision had to move. The parser cannot tell these apart.
#[tokio::test]
async fn a_utility_statement_in_a_missing_schema_reports_the_schema() {
    let (_engine, mut s) = engine_with(&[]).await;
    for sql in [
        "CREATE TABLE nope.t (x int)",
        "CREATE TABLE IF NOT EXISTS nope.t (x int)",
        "CREATE TABLE nope.t AS SELECT 1 AS x",
        "CREATE TABLE t2 (LIKE nope.t)",
        "CREATE VIEW nope.v AS SELECT 1",
        "CREATE INDEX i ON nope.t (x)",
        "CREATE SEQUENCE nope.s",
        "CREATE DOMAIN nope.d AS int",
        "CREATE TYPE nope.ty AS (a int)",
        "ALTER TABLE nope.t ADD COLUMN y int",
        "DROP TABLE nope.t",
        "DROP VIEW nope.v",
        "DROP INDEX nope.i",
        "DROP SEQUENCE nope.s",
        "TRUNCATE nope.t",
        "GRANT SELECT ON TABLE nope.t TO postgres",
        "REVOKE SELECT ON TABLE nope.t FROM postgres",
    ] {
        assert!(
            failure_of(&mut s, sql).await == schema_missing("nope"),
            "{sql}"
        );
    }
}

/// `IF EXISTS` skips a name whose schema is missing rather than reporting it,
/// and still acts on the rest of the list. 18.4 does the same, with a NOTICE
/// this engine has no channel for.
#[tokio::test]
async fn if_exists_skips_a_name_in_a_missing_schema() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE keep (x int)"]).await;
    run(&mut s, "DROP TABLE IF EXISTS nope.t").await;
    run(&mut s, "DROP VIEW IF EXISTS nope.v").await;
    run(&mut s, "DROP INDEX IF EXISTS nope.i").await;
    run(&mut s, "ALTER TABLE IF EXISTS nope.t ADD COLUMN y int").await;
    // The rest of the list is still dropped, exactly as 18.4 does.
    run(&mut s, "DROP TABLE IF EXISTS nope.t, keep").await;
    assert!(
        query(
            &mut s,
            "SELECT count(*) FROM pg_class WHERE relname = 'keep'"
        )
        .await
            == vec![text_row(&["0"])]
    );
}

/// A schema that exists but holds nothing reports the *relation*, from every
/// statement form. The schema check only fires when the schema is absent.
#[tokio::test]
async fn an_existing_schema_without_the_relation_is_an_undefined_relation() {
    let (_engine, mut s) = engine_with(&["CREATE SCHEMA s1"]).await;
    assert!(failure_of(&mut s, "SELECT * FROM s1.t").await == relation_missing("s1.t"));
    assert!(failure_of(&mut s, "TRUNCATE s1.t").await == relation_missing("s1.t"));
    // 18.4 names just `t` in `DROP TABLE`'s message, where the catalog's
    // still-flattened name is what this engine has to report. That is a
    // property of the one-part key, not of this change.
    assert!(
        failure_of(&mut s, "DROP TABLE s1.t").await
            == Failure("42P01".into(), "table \"s1.t\" does not exist".into())
    );
}

/// A `public` qualifier reaches the relation the bare name reaches, because
/// `public` is where the default `search_path` puts an unqualified name. The
/// unquoted `PUBLIC` folds to it. This is resolution through the path, not a
/// special case for one schema name.
#[tokio::test]
async fn a_public_qualifier_reaches_the_relation_the_search_path_reaches() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (x int)"]).await;
    run(&mut s, "INSERT INTO public.t VALUES (1)").await;
    run(&mut s, "INSERT INTO PUBLIC.t VALUES (3)").await;
    assert!(
        query(&mut s, "SELECT x FROM t ORDER BY x").await
            == vec![text_row(&["1"]), text_row(&["3"])]
    );
    assert!(
        query(&mut s, "SELECT x FROM public.t ORDER BY x").await
            == vec![text_row(&["1"]), text_row(&["3"])]
    );
    run(&mut s, "UPDATE public.t SET x = x + 10").await;
    assert!(
        query(&mut s, "SELECT x FROM t ORDER BY x").await
            == vec![text_row(&["11"]), text_row(&["13"])]
    );
    run(&mut s, "TRUNCATE public.t").await;
    assert!(query(&mut s, "SELECT x FROM t").await.is_empty());
    run(&mut s, "DROP TABLE public.t").await;
    assert!(failure_of(&mut s, "SELECT x FROM t").await == relation_missing("t"));
}

/// `pg_temp` names the session's own temporary namespace, not `public`, so a
/// permanent relation is not reachable through it. A session that has created
/// no temporary relation has no such namespace. 18.4 then splits the report the
/// same way it splits every missing schema. A DML target reports the dotted
/// relation, and a utility statement reports the schema.
#[tokio::test]
async fn a_pg_temp_qualifier_does_not_reach_a_permanent_relation() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (x int)"]).await;
    for sql in [
        "SELECT x FROM pg_temp.t",
        "INSERT INTO pg_temp.t VALUES (2)",
        "UPDATE pg_temp.t SET x = 1",
        "DELETE FROM pg_temp.t WHERE x = 1",
    ] {
        assert!(
            failure_of(&mut s, sql).await == relation_missing("pg_temp.t"),
            "{sql}"
        );
    }
    for sql in ["TRUNCATE pg_temp.t", "DROP TABLE pg_temp.t"] {
        assert!(
            failure_of(&mut s, sql).await == schema_missing("pg_temp"),
            "{sql}"
        );
    }
    // The permanent relation is untouched by any of it.
    run(&mut s, "INSERT INTO t VALUES (1)").await;
    assert!(query(&mut s, "SELECT x FROM t").await == vec![text_row(&["1"])]);
}

/// The catalog qualifiers keep resolving by their dotted names, which is what
/// reaches the virtual-catalog path rather than the stored one.
#[tokio::test]
async fn catalog_qualifiers_still_reach_the_virtual_catalog() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (x int)"]).await;
    assert!(
        query(
            &mut s,
            "SELECT relname FROM pg_catalog.pg_class WHERE relname = 't'"
        )
        .await
            == vec![text_row(&["t"])]
    );
    assert!(
        query(
            &mut s,
            "SELECT table_name FROM information_schema.tables WHERE table_name = 't'"
        )
        .await
            == vec![text_row(&["t"])]
    );
}

/// `CREATE VIEW public.v` was a bare syntax error before this change. The view
/// path was the one relation name with no dot handling at all.
#[tokio::test]
async fn create_view_accepts_a_qualified_name() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE t (x int)", "INSERT INTO t VALUES (7)"]).await;
    run(&mut s, "CREATE VIEW public.v AS SELECT x FROM public.t").await;
    assert!(query(&mut s, "SELECT x FROM public.v").await == vec![text_row(&["7"])]);
    assert!(query(&mut s, "SELECT x FROM v").await == vec![text_row(&["7"])]);
    run(&mut s, "DROP VIEW public.v").await;
    assert!(failure_of(&mut s, "SELECT x FROM v").await == relation_missing("v"));
}

/// A three-part name can only ever be the cross-database refusal here. The
/// engine has one database, so there is nothing else `a.b.c` could mean.
#[tokio::test]
async fn a_three_part_name_is_the_cross_database_refusal() {
    let (_engine, mut s) = engine_with(&[]).await;
    for sql in ["SELECT * FROM db.s.t", "DROP TABLE db.s.t"] {
        assert!(
            failure_of(&mut s, sql).await
                == Failure(
                    "0A000".into(),
                    "cross-database references are not implemented: \"db.s.t\"".into()
                ),
            "{sql}"
        );
    }
}

/// A qualifier is never a CTE reference. `public.t` names the stored relation
/// even where a CTE of that name is in scope.
#[tokio::test]
async fn a_qualified_name_never_resolves_to_a_cte() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE t (x int)", "INSERT INTO t VALUES (1)"]).await;
    assert!(
        query(&mut s, "WITH t AS (SELECT 99 AS x) SELECT x FROM public.t").await
            == vec![text_row(&["1"])]
    );
    assert!(
        query(&mut s, "WITH t AS (SELECT 99 AS x) SELECT x FROM t").await
            == vec![text_row(&["99"])]
    );
}

/// The error names the case-folded, dotted, unquoted form rather than the
/// source text. The lexer folds unquoted identifiers, so a `RelationRef` built
/// from its tokens renders correctly with no extra machinery.
#[tokio::test]
async fn the_missing_relation_is_named_case_folded() {
    let (_engine, mut s) = engine_with(&[]).await;
    assert!(failure_of(&mut s, "SELECT * FROM S.T").await == relation_missing("s.t"));
    assert!(failure_of(&mut s, "DROP TABLE S.T").await == schema_missing("s"));
}

/// A relation in a real schema is reachable, and by its qualified name only.
/// That is what the one parse policy buys: `CREATE`, `INSERT` and `SELECT` all
/// agree on what the dot meant.
#[tokio::test]
async fn a_relation_in_a_created_schema_round_trips_through_every_statement() {
    let (_engine, mut s) = engine_with(&["CREATE SCHEMA s1"]).await;
    run(&mut s, "CREATE TABLE s1.t (x int)").await;
    run(&mut s, "INSERT INTO s1.t VALUES (1), (2)").await;
    run(&mut s, "UPDATE s1.t SET x = x + 10 WHERE x = 1").await;
    run(&mut s, "DELETE FROM s1.t WHERE x = 2").await;
    assert!(query(&mut s, "SELECT x FROM s1.t").await == vec![text_row(&["11"])]);
    // The bare name is a different relation, and there is none.
    assert!(failure_of(&mut s, "SELECT * FROM t").await == relation_missing("t"));
    run(&mut s, "TRUNCATE s1.t").await;
    run(&mut s, "DROP TABLE s1.t").await;
    assert!(failure_of(&mut s, "SELECT * FROM s1.t").await == relation_missing("s1.t"));
}
