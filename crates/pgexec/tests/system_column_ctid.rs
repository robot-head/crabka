//! The system columns: `ctid` over a stored relation and over one the engine
//! synthesises, and the six names no relation with storage may declare a column
//! of.
//!
//! No test here asserts a particular `ctid`. The value is implementation
//! defined — `PostgreSQL` moves it on `UPDATE` and renumbers every one of them
//! on `CLUSTER` — so what is pinned is the contract around it: that the name
//! resolves where a relation has rows, that two rows of one relation differ,
//! that a row keeps its own across reads, and that every enumeration of a
//! relation's columns still leaves it out.

use assert2::assert;
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

/// The whole result as text.
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

/// Every row of a single-column result, in order.
async fn column(engine: &SqlEngine, sql: &str) -> Vec<Option<String>> {
    grid(engine, sql)
        .await
        .into_iter()
        .map(|row| row.into_iter().next().expect("one column"))
        .collect()
}

/// The error a statement reports, or `None` when it succeeds.
async fn error_of(engine: &SqlEngine, sql: &str) -> Option<String> {
    match engine.connect().simple_query(sql).await {
        Ok(_) => None,
        Err(error) => Some(error.to_string()),
    }
}

fn text_of(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("valid text cell")
}

async fn fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (a int, b text)").await;
    run(&engine, "INSERT INTO t VALUES (1, 'one'), (2, 'two')").await;
    run(&engine, "CREATE VIEW v AS SELECT a FROM t").await;
    engine
}

/// `drop_operator`'s own query: an anti-join over `pg_operator` that names
/// `ctid` only to have something to report an offending row by. Nothing is
/// offending, so the answer is no rows — but the column has to resolve for the
/// statement to run at all.
#[tokio::test]
async fn the_catalog_anti_join_that_names_ctid_answers_no_rows() {
    let engine = fixture().await;
    for reference in ["oprcom", "oprnegate"] {
        let sql = format!(
            "SELECT ctid, {reference} FROM pg_catalog.pg_operator fk \
             WHERE {reference} != 0 AND NOT EXISTS \
             (SELECT 1 FROM pg_catalog.pg_operator pk WHERE pk.oid = fk.{reference})"
        );
        assert!(
            grid(&engine, &sql).await == Vec::<Vec<Option<String>>>::new(),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn ctid_resolves_bare_and_qualified_on_a_stored_and_a_catalog_relation() {
    let engine = fixture().await;
    let cases = [
        ("SELECT ctid FROM t", 2),
        ("SELECT t.ctid FROM t", 2),
        ("SELECT ctid FROM t AS q", 2),
        ("SELECT q.ctid FROM t AS q", 2),
        ("SELECT ctid FROM pg_catalog.pg_operator LIMIT 3", 3),
        ("SELECT fk.ctid FROM pg_catalog.pg_operator fk LIMIT 3", 3),
        ("SELECT ctid FROM pg_catalog.pg_class LIMIT 3", 3),
    ];
    for (sql, expected) in cases {
        let values = column(&engine, sql).await;
        assert!(values.len() == expected, "{sql}");
        assert!(values.iter().all(Option::is_some), "{sql}");
    }
}

/// A relation the engine holds no rows for has no `ctid` to answer with, which
/// is `PostgreSQL`'s answer for a view and for anything derived.
#[tokio::test]
async fn ctid_is_undefined_where_no_relation_stores_the_row() {
    let engine = fixture().await;
    let cases = [
        "SELECT ctid FROM v",
        "SELECT ctid FROM (SELECT a FROM t) s",
        "SELECT ctid FROM (VALUES (1)) AS s(a)",
        "WITH c AS (SELECT a FROM t) SELECT ctid FROM c",
    ];
    for sql in cases {
        let error = error_of(&engine, sql).await;
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains(r#"column "ctid" does not exist"#)),
            "{sql} answered {error:?}"
        );
    }
}

#[tokio::test]
async fn two_rows_of_one_relation_take_different_ctids() {
    let engine = fixture().await;
    let stored = column(&engine, "SELECT ctid FROM t").await;
    assert!(stored.len() == 2);
    assert!(stored[0] != stored[1]);

    let synthesised = column(&engine, "SELECT ctid FROM pg_catalog.pg_operator LIMIT 5").await;
    let distinct: std::collections::BTreeSet<_> = synthesised.iter().collect();
    assert!(distinct.len() == synthesised.len());
}

#[tokio::test]
async fn a_row_keeps_its_ctid_across_two_reads() {
    let engine = fixture().await;
    for sql in [
        "SELECT a, ctid FROM t ORDER BY a",
        "SELECT oid, ctid FROM pg_catalog.pg_operator ORDER BY oid LIMIT 5",
    ] {
        let first = grid(&engine, sql).await;
        let second = grid(&engine, sql).await;
        assert!(!first.is_empty(), "{sql}");
        assert!(first == second, "{sql}");
    }
}

/// A system column is invisible to every enumeration of a relation's columns.
#[tokio::test]
async fn ctid_is_absent_from_every_enumeration_of_a_relations_columns() {
    let engine = fixture().await;
    // `SELECT *`, `SELECT t.*` and a whole-row `SELECT t` expand the relation's
    // own columns and no more; the catalog-driven ones are what `\d` and an ORM
    // preamble read.
    let cases = [
        "SELECT * FROM t",
        "SELECT t.* FROM t",
        "SELECT t FROM t",
        "SELECT attname FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 't'",
        "SELECT column_name FROM information_schema.columns WHERE table_name = 't'",
        "SELECT attname FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 'pg_operator'",
    ];
    for sql in cases {
        let rendered = format!("{:?}", grid(&engine, sql).await);
        assert!(
            !rendered.contains("ctid"),
            "{sql} showed ctid in {rendered}"
        );
    }
}

/// A bare system column inside a subquery belongs to the subquery's own FROM.
///
/// The name-shadowing pass that tells a correlated subquery from an
/// uncorrelated one described the inner FROM without its system columns, so a
/// bare `ctid` there looked like a reference to the enclosing row and was bound
/// to it. `WHERE ctid IN (SELECT ctid FROM …)` then compared every row against
/// its own `ctid` and admitted the lot.
#[tokio::test]
async fn a_bare_ctid_in_a_subquery_reads_the_subquerys_own_relation() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE s (a int)").await;
    run(&engine, "INSERT INTO s VALUES (1), (2)").await;
    // Each spelling of "the second row of the other relation" selects exactly
    // the row of `t` sitting at the same identity, and no other.
    let filters = [
        "ctid IN (SELECT ctid FROM s WHERE a = 2)",
        "ctid IN (SELECT ctid FROM t u WHERE u.a = 2)",
        "ctid = (SELECT ctid FROM s WHERE a = 2)",
        "t.ctid IN (SELECT ctid FROM s WHERE a = 2)",
    ];
    for filter in filters {
        let sql = format!("SELECT a FROM t WHERE {filter} ORDER BY a");
        assert!(
            column(&engine, &sql).await == vec![Some("2".to_string())],
            "{sql}"
        );
    }
    // A scalar subquery over another relation is one value for the whole
    // statement, not the source row's own.
    let scalar = column(
        &engine,
        "SELECT (SELECT ctid FROM s WHERE a = 2) FROM t ORDER BY a",
    )
    .await;
    assert!(scalar.len() == 2);
    assert!(scalar[0] == scalar[1]);
}

/// The column has to stay hidden even when the same statement reads it, since
/// that is the one time it is in the scope at all.
#[tokio::test]
async fn a_statement_that_reads_ctid_still_does_not_expand_it() {
    let engine = fixture().await;
    let cases = [
        ("SELECT *, ctid FROM t ORDER BY a", 3),
        ("SELECT t.*, t.ctid FROM t ORDER BY a", 3),
        ("SELECT t, ctid FROM t ORDER BY a", 2),
    ];
    for (sql, width) in cases {
        let rows = grid(&engine, sql).await;
        assert!(rows.len() == 2, "{sql}");
        assert!(rows.iter().all(|row| row.len() == width), "{sql} {rows:?}");
    }
    // The whole-row value is the relation's own columns, with no system column
    // stitched onto the end of the composite.
    let whole = grid(&engine, "SELECT t, ctid FROM t ORDER BY a").await;
    assert!(whole[0][0] == Some("(1,one)".to_string()));
}

/// No relation with storage may declare a column named after a system column.
///
/// `PostgreSQL` raises this in `CheckAttributeNamesTypes`, which covers every
/// relkind except a view and a composite type, and the message is quoted
/// verbatim by `errors.sql` and `alter_table.sql`.
#[tokio::test]
async fn a_relation_with_storage_may_not_declare_a_system_column_name() {
    let engine = fixture().await;
    // Every one of the six, and every DDL path that can name a column.
    let refused = [
        "CREATE TABLE bad (ctid int)",
        "CREATE TABLE bad (xmin int)",
        "CREATE TABLE bad (xmax int)",
        "CREATE TABLE bad (cmin int)",
        "CREATE TABLE bad (cmax int)",
        "CREATE TABLE bad (a int, tableoid int)",
        "CREATE TABLE bad AS SELECT 1 AS ctid",
        "SELECT 1 AS xmin INTO bad",
        "CREATE MATERIALIZED VIEW bad AS SELECT 1 AS ctid",
        "CREATE TABLE bad (a int) PARTITION BY RANGE (ctid)",
        "ALTER TABLE t ADD COLUMN xmin integer",
        // Refused even under IF NOT EXISTS: the name is taken by something the
        // clause cannot decide it already added.
        "ALTER TABLE t ADD COLUMN IF NOT EXISTS ctid integer",
        // The route that stays open once every creation path is closed.
        "ALTER TABLE t RENAME COLUMN a TO ctid",
    ];
    for sql in refused {
        let error = error_of(&engine, sql).await;
        assert!(
            error.as_deref().is_some_and(|error| {
                error.contains("conflicts with a system column name")
                    || error.contains("cannot use system column")
            }),
            "{sql} answered {error:?}"
        );
    }
    // A view has no system attributes to collide with, so `PostgreSQL` exempts
    // it — `tid.sql` creates exactly this one — and the column it declares is
    // an ordinary column of the view.
    assert!(error_of(&engine, "CREATE VIEW fake AS SELECT 1 AS ctid, 2 AS a").await == None);
    assert!(
        grid(&engine, "SELECT * FROM fake").await
            == vec![vec![Some("1".to_string()), Some("2".to_string())]]
    );
    // Nothing about the rule touches an ordinary name.
    for sql in [
        "CREATE TABLE fine (a int, b text)",
        "ALTER TABLE t ADD COLUMN c int",
        "ALTER TABLE t RENAME COLUMN c TO d",
    ] {
        assert!(error_of(&engine, sql).await == None, "{sql}");
    }
}
