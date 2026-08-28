//! The system columns: `ctid` over a stored relation and over one the engine
//! synthesises, `ctid` in a statement that writes, and the six names no
//! relation with storage may declare a column of.
//!
//! Almost no test here asserts a particular `ctid`. The value is implementation
//! defined — `PostgreSQL` moves it on `UPDATE` and renumbers every one of them
//! on `CLUSTER` — so what is pinned is the contract around it: that the name
//! resolves where a relation has rows, that two rows of one relation differ,
//! that a row keeps its own across reads, and that every enumeration of a
//! relation's columns still leaves it out.
//!
//! The write tests are the exception, and they are the reason for the rule.
//! A `ctid` a statement only prints can be wrong and merely print wrongly; a
//! `ctid` in a `WHERE` decides which row is destroyed. So those tests read a
//! row's `ctid` back, write against exactly that value, and assert which rows
//! are left — never that the value itself was any particular one.

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

/// **The test the write path exists to pass.** A `ctid` in a `DELETE`'s `WHERE`
/// must name the row the engine is about to destroy, and no other.
///
/// Getting this wrong in a predicate is worse than the 42703 it replaced: a
/// statement that refused to run left every row in place, and one that resolves
/// the column to the wrong slot removes the wrong row and reports success. So
/// the `ctid` is read back off a known row first, and what is asserted is which
/// rows survive.
#[tokio::test]
async fn a_delete_by_ctid_removes_exactly_the_row_that_ctid_names() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE many (a int)").await;
    run(
        &engine,
        "INSERT INTO many VALUES (10), (20), (30), (40), (50)",
    )
    .await;
    let third = column(&engine, "SELECT ctid FROM many WHERE a = 30")
        .await
        .into_iter()
        .next()
        .flatten()
        .expect("the row has a ctid");

    let deleted = column(
        &engine,
        &format!("DELETE FROM many WHERE ctid = '{third}' RETURNING a"),
    )
    .await;
    assert!(deleted == vec![Some("30".to_string())]);
    assert!(
        column(&engine, "SELECT a FROM many ORDER BY a").await
            == ["10", "20", "40", "50"]
                .map(|a| Some(a.to_string()))
                .to_vec()
    );
}

/// The same for an `UPDATE`, and for the shapes a `ctid` can be buried in: an
/// expression over `ctid::text`, a subquery, and a `USING` join.
#[tokio::test]
async fn a_write_reaches_the_row_a_ctid_names_in_every_shape() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE many (a int, b text)").await;
    run(
        &engine,
        "INSERT INTO many VALUES (10,'j'), (20,'k'), (30,'l'), (40,'m')",
    )
    .await;
    let ctid_of = |a: i32| {
        let engine = &engine;
        async move {
            column(engine, &format!("SELECT ctid FROM many WHERE a = {a}"))
                .await
                .into_iter()
                .next()
                .flatten()
                .expect("the row has a ctid")
        }
    };

    let second = ctid_of(20).await;
    run(
        &engine,
        &format!("UPDATE many SET b = 'changed' WHERE ctid = '{second}'"),
    )
    .await;
    assert!(
        grid(&engine, "SELECT a, b FROM many ORDER BY a").await
            == [("10", "j"), ("20", "changed"), ("30", "l"), ("40", "m")]
                .map(|(a, b)| vec![Some(a.to_string()), Some(b.to_string())])
                .to_vec()
    );

    // A subquery over another relation, which is what a `WHERE ctid IN (SELECT
    // ctid FROM …)` reduces to once the inner query binds to its own FROM.
    let third = ctid_of(30).await;
    run(&engine, "CREATE TABLE picks (t tid)").await;
    run(&engine, &format!("INSERT INTO picks VALUES ('{third}')")).await;
    run(
        &engine,
        "DELETE FROM many WHERE ctid IN (SELECT t FROM picks)",
    )
    .await;
    assert!(
        column(&engine, "SELECT a FROM many ORDER BY a").await
            == ["10", "20", "40"].map(|a| Some(a.to_string())).to_vec()
    );

    // A `USING` join whose join key is the target's own `ctid`.
    let first = ctid_of(10).await;
    run(&engine, &format!("INSERT INTO picks VALUES ('{first}')")).await;
    run(
        &engine,
        "DELETE FROM many USING picks WHERE many.ctid = picks.t AND many.a = 10",
    )
    .await;
    assert!(
        column(&engine, "SELECT a FROM many ORDER BY a").await
            == ["20", "40"].map(|a| Some(a.to_string())).to_vec()
    );
}

/// `tidrangescan`'s own `DELETE`: an expression that renders the `ctid` as text
/// and reads its offset back out, which is how upstream trims each page down to
/// its first ten tuples.
///
/// The rows this leaves are not the rows `PostgreSQL` is left with — there is no
/// heap here, so every row of a small relation sits in block 0 — but the
/// statement has to reach the rows its predicate names, and it has to leave the
/// rest.
#[tokio::test]
async fn a_delete_filters_on_an_expression_over_the_ctid() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE r (id int)").await;
    run(
        &engine,
        "INSERT INTO r SELECT i FROM generate_series(1, 25) AS s(i)",
    )
    .await;
    let survivors: Vec<Option<String>> = column(
        &engine,
        "SELECT id FROM r WHERE substring(ctid::text FROM ',(\\d+)\\)')::integer <= 10 ORDER BY id",
    )
    .await;
    assert!(survivors.len() == 10);

    run(
        &engine,
        "DELETE FROM r WHERE substring(ctid::text FROM ',(\\d+)\\)')::integer > 10 \
         OR substring(ctid::text FROM '\\((\\d+),')::integer > 2",
    )
    .await;
    assert!(column(&engine, "SELECT id FROM r ORDER BY id").await == survivors);
}

/// `RETURNING` projects the `ctid` of the row the statement wrote, whichever of
/// the three statements wrote it, and through either spelling.
///
/// UPDATE writes a fresh heap tuple, so `old.ctid` and `new.ctid` differ.
/// Nothing may depend on either — a `ctid` is documented as valid only until
/// the row is updated.
#[tokio::test]
async fn returning_projects_the_ctid_of_the_row_that_was_written() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE w (a int, b text)").await;

    let inserted = grid(&engine, "INSERT INTO w VALUES (1, 'one') RETURNING ctid, a").await;
    assert!(inserted.len() == 1);
    let written = inserted[0][0].clone().expect("a ctid");
    assert!(inserted[0][1] == Some("1".to_string()));
    // The value the write reported is the value a read of that row gives back.
    assert!(column(&engine, "SELECT ctid FROM w WHERE a = 1").await == vec![Some(written.clone())]);

    let updated = grid(
        &engine,
        "UPDATE w SET b = 'two' WHERE a = 1 RETURNING ctid, old.ctid, new.ctid, old.b, new.b",
    )
    .await;
    assert!(updated.len() == 1, "{updated:?}");
    let replacement = updated[0][0].clone().expect("a replacement ctid");
    assert!(updated[0][0] == updated[0][2]);
    assert!(updated[0][1] == Some(written));
    assert!(updated[0][0] != updated[0][1]);
    assert!(updated[0][3] == Some("one".to_string()));
    assert!(updated[0][4] == Some("two".to_string()));

    let deleted = grid(
        &engine,
        "DELETE FROM w WHERE a = 1 RETURNING ctid, old.ctid, a",
    )
    .await;
    assert!(
        deleted
            == vec![vec![
                Some(replacement.clone()),
                Some(replacement),
                Some("1".to_string())
            ]]
    );
}

/// A partitioned INSERT writes one leaf, so its `RETURNING` system columns
/// describe that leaf rather than the storage-less partitioned parent.
#[tokio::test]
async fn partitioned_insert_returning_uses_the_leaf_system_columns() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE p (a int) PARTITION BY RANGE (a)").await;
    run(
        &engine,
        "CREATE TABLE p0 PARTITION OF p FOR VALUES FROM (0) TO (10)",
    )
    .await;

    let returned = grid(
        &engine,
        "INSERT INTO p VALUES (1) RETURNING tableoid::regclass, xmin::text, ctid, a",
    )
    .await;
    assert!(returned.len() == 1, "{returned:?}");
    assert!(returned[0][0] == Some("p0".to_string()));
    assert!(returned[0][1].is_some());
    assert!(returned[0][2].is_some());
    assert!(returned[0][3] == Some("1".to_string()));
    assert!(
        grid(
            &engine,
            "SELECT tableoid::regclass, xmin::text, ctid, a FROM p"
        )
        .await
            == returned
    );
}

/// `xmin` is the creating transaction ID carried by the stored tuple, so an
/// INSERT's `RETURNING` value is the value a later scan reads back.
#[tokio::test]
async fn xmin_is_visible_on_insert_returning_and_a_later_scan() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE xmin_rows (a int)").await;

    let inserted = grid(
        &engine,
        "INSERT INTO xmin_rows VALUES (1) RETURNING xmin::text",
    )
    .await;
    assert!(inserted.len() == 1, "{inserted:?}");
    assert!(inserted[0][0].is_some());
    assert!(grid(&engine, "SELECT xmin::text FROM xmin_rows").await == inserted);
}

/// An UPDATE creates the visible tuple version, while its OLD image keeps the
/// version that the statement replaced.
#[tokio::test]
async fn update_returning_reports_the_old_and_new_xmin_versions() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE xmin_updates (a int)").await;
    let inserted = grid(
        &engine,
        "INSERT INTO xmin_updates VALUES (1) RETURNING xmin::text",
    )
    .await;
    let updated = grid(
        &engine,
        "UPDATE xmin_updates SET a = 2 RETURNING xmin::text, old.xmin::text, new.xmin::text",
    )
    .await;
    assert!(updated.len() == 1, "{updated:?}");
    assert!(updated[0][0] == updated[0][2]);
    assert!(updated[0][1] == inserted[0][0]);
    assert!(updated[0][0] != inserted[0][0]);
    assert!(
        grid(&engine, "SELECT xmin::text FROM xmin_updates").await
            == vec![vec![updated[0][0].clone()]]
    );
}

/// UPDATE writes a successor tuple at the heap tail. Its `ctid` differs from
/// the OLD image and is the value subsequent scans return.
#[tokio::test]
async fn update_returning_reports_distinct_old_and_new_ctids() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE ctid_updates (a int)").await;
    run(&engine, "INSERT INTO ctid_updates VALUES (1)").await;

    let returned = grid(
        &engine,
        "UPDATE ctid_updates SET a = 2 RETURNING ctid, old.ctid, new.ctid",
    )
    .await;
    assert!(returned.len() == 1, "{returned:?}");
    assert!(returned[0][0] == returned[0][2]);
    assert!(returned[0][0] != returned[0][1]);
    assert!(
        grid(&engine, "SELECT ctid FROM ctid_updates").await == vec![vec![returned[0][2].clone()]]
    );
}

/// Each command in a transaction stamps the replacement tuple with its own
/// command ID. A second UPDATE must not retain the INSERT's `cmin`.
#[tokio::test]
async fn update_replaces_the_visible_tuples_command_id() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE command_versions (a int)").await;
    let mut session = engine.connect();
    for sql in [
        "BEGIN",
        "INSERT INTO command_versions VALUES (1)",
        "UPDATE command_versions SET a = 2",
        "UPDATE command_versions SET a = 3",
    ] {
        session.simple_query(sql).await.expect("command succeeds");
    }
    let QueryResult::Rows { rows, .. } = session
        .simple_query("SELECT cmin::text, cmax::text, xmax::text FROM command_versions")
        .await
        .expect("system-column scan")
        .pop()
        .expect("one result")
    else {
        panic!("expected rows");
    };
    assert!(
        rows.into_iter()
            .map(|row| row
                .into_iter()
                .map(|cell| cell.map(|cell| text_of(&cell)))
                .collect())
            .collect::<Vec<Vec<Option<String>>>>()
            == vec![vec![
                Some("2".to_string()),
                Some("0".to_string()),
                Some("0".to_string()),
            ]]
    );
    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback succeeds");
}

/// `RETURNING` exposes the header each image carries: the OLD image receives
/// this UPDATE's command xmax while the NEW image starts at this command.
#[tokio::test]
async fn update_returning_reports_command_ids_for_old_and_new_images() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE returning_commands (a int)").await;
    let mut session = engine.connect();
    for sql in ["BEGIN", "INSERT INTO returning_commands VALUES (1)"] {
        session.simple_query(sql).await.expect("command succeeds");
    }
    let QueryResult::Rows { rows, .. } = session
        .simple_query(
            "UPDATE returning_commands SET a = 2 \
             RETURNING cmin::text, cmax::text, old.cmin::text, old.cmax::text, \
                       new.cmin::text, new.cmax::text",
        )
        .await
        .expect("update succeeds")
        .pop()
        .expect("one result")
    else {
        panic!("expected rows");
    };
    assert!(
        rows.into_iter()
            .map(|row| row
                .into_iter()
                .map(|cell| cell.map(|cell| text_of(&cell)))
                .collect())
            .collect::<Vec<Vec<Option<String>>>>()
            == vec![vec![
                Some("1".to_string()),
                Some("0".to_string()),
                Some("0".to_string()),
                Some("1".to_string()),
                Some("1".to_string()),
                Some("0".to_string()),
            ]]
    );
    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback succeeds");
}

/// A system column stays out of every expansion a `RETURNING` clause makes,
/// including the two an image alias adds.
#[tokio::test]
async fn returning_still_does_not_expand_a_system_column() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE w (a int, b text)").await;
    run(&engine, "INSERT INTO w VALUES (1, 'one')").await;
    // Two visible columns per expansion and one for the named `ctid`: seven.
    let rows = grid(
        &engine,
        "UPDATE w SET b = 'two' WHERE a = 1 RETURNING ctid, *, old.*, new.*",
    )
    .await;
    assert!(rows.len() == 1);
    assert!(rows[0].len() == 7, "{rows:?}");
}

/// `EXCLUDED` is the row the `INSERT` proposed, which is not stored and has no
/// identity, so `PostgreSQL` refuses a system column on it — the point
/// `insert_conflict.sql` makes. Naming `ctid` elsewhere in the statement, which
/// is what turns the column on for the target, does not open it here.
///
/// The message differs from upstream's `column excluded.ctid does not exist`:
/// this engine reports an unresolved qualified reference by its bare name
/// everywhere, which is a separate gap. What is pinned is the refusal.
#[tokio::test]
async fn excluded_offers_no_system_column_however_the_statement_spells_one() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE k (key int4 UNIQUE, data text)").await;
    run(&engine, "INSERT INTO k VALUES (1, 'first')").await;
    for sql in [
        "INSERT INTO k VALUES (1) ON CONFLICT (key) DO UPDATE SET data = excluded.ctid::text",
        "INSERT INTO k VALUES (1) ON CONFLICT (key) DO UPDATE \
         SET data = excluded.ctid::text RETURNING ctid",
    ] {
        let error = error_of(&engine, sql).await;
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains(r#"column "ctid" does not exist"#)),
            "{sql} answered {error:?}"
        );
    }
    // The conflicting row was left alone by both refusals.
    assert!(
        grid(&engine, "SELECT key, data FROM k").await
            == vec![vec![Some("1".to_string()), Some("first".to_string())]]
    );
}

/// A `SET` list may not assign a system column. `PostgreSQL` resolves the name,
/// finds a system attribute and reports 0A000 — not the 42703 an unknown column
/// gets, which would be a poor answer about a column the same statement can
/// read in its `WHERE`.
#[tokio::test]
async fn a_set_list_may_not_assign_a_system_column() {
    let engine = fixture().await;
    for name in ["ctid", "tableoid", "xmin"] {
        let error = error_of(&engine, &format!("UPDATE t SET {name} = NULL WHERE a = 1")).await;
        assert!(
            error.as_deref().is_some_and(
                |error| error.contains(&format!("cannot assign to system column \"{name}\""))
            ),
            "{name} answered {error:?}"
        );
    }
    // A view may declare a column of the name, and then its own updatability
    // rule answers instead — which is the message `updatable_views.sql` wants.
    run(&engine, "CREATE VIEW vc AS SELECT ctid, a FROM t").await;
    let error = error_of(&engine, "UPDATE vc SET ctid = NULL WHERE a = 1").await;
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("cannot update column \"ctid\" of view")),
        "{error:?}"
    );
}

/// A relation the engine holds no rows for still has no `ctid` on the write
/// path, which is the read path's rule and the reason a view target is given
/// none: its rows come out of its own query and carry no identity.
#[tokio::test]
async fn a_write_to_a_view_has_no_ctid_of_its_own() {
    let engine = fixture().await;
    run(&engine, "CREATE VIEW vw AS SELECT a, b FROM t").await;
    for sql in [
        "UPDATE vw SET b = 'x' WHERE ctid = '(0,1)'",
        "DELETE FROM vw WHERE ctid = '(0,1)'",
        "UPDATE vw SET b = 'x' WHERE a = 1 RETURNING ctid",
    ] {
        let error = error_of(&engine, sql).await;
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains(r#"column "ctid" does not exist"#)),
            "{sql} answered {error:?}"
        );
    }
    // The rows are untouched: a statement that cannot resolve its predicate
    // writes nothing.
    assert!(
        grid(&engine, "SELECT a, b FROM t ORDER BY a").await
            == vec![
                vec![Some("1".to_string()), Some("one".to_string())],
                vec![Some("2".to_string()), Some("two".to_string())],
            ]
    );
}

/// A data-modifying `WITH` entry runs as its own statement, so its own `ctid`
/// reference is its own to resolve.
#[tokio::test]
async fn a_data_modifying_cte_resolves_its_own_ctid() {
    let engine = fixture().await;
    run(&engine, "CREATE TABLE c (a int)").await;
    run(&engine, "INSERT INTO c VALUES (1), (2), (3)").await;
    let second = column(&engine, "SELECT ctid FROM c WHERE a = 2")
        .await
        .into_iter()
        .next()
        .flatten()
        .expect("the row has a ctid");
    let out = column(
        &engine,
        &format!("WITH d AS (DELETE FROM c WHERE ctid = '{second}' RETURNING a) SELECT a FROM d"),
    )
    .await;
    assert!(out == vec![Some("2".to_string())]);
    assert!(
        column(&engine, "SELECT a FROM c ORDER BY a").await
            == vec![Some("1".to_string()), Some("3".to_string())]
    );
}

/// A row-security policy that names `ctid` judges the rows a write may reach,
/// and it does so whether or not the statement itself spells the column.
///
/// The write path binds a policy qual against the target's own columns and
/// nothing else, so a policy naming a system column has no binding to reach and
/// the statement is refused. That is the safe direction — a refused `DELETE`
/// removes nothing — and it is pinned here so a later slice that widens the
/// write scope cannot quietly turn the refusal into an admission.
#[tokio::test]
async fn a_policy_that_names_ctid_never_admits_a_write_it_cannot_judge() {
    let engine = fixture().await;
    let mut owner = engine.connect();
    for sql in [
        "CREATE TABLE guarded (a int, b text)",
        "INSERT INTO guarded VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY p ON guarded AS PERMISSIVE USING (ctid = '(0,1)')",
        "CREATE ROLE reader",
        "GRANT SELECT, UPDATE, DELETE ON guarded TO reader",
    ] {
        owner.simple_query(sql).await.expect("setup succeeds");
    }
    let mut reader = engine.connect();
    reader
        .simple_query("SET ROLE reader")
        .await
        .expect("set role");
    for sql in [
        "DELETE FROM guarded",
        "DELETE FROM guarded WHERE ctid = '(0,3)'",
        "UPDATE guarded SET b = 'x'",
        "UPDATE guarded SET b = 'x' WHERE ctid = '(0,3)'",
    ] {
        let error = reader.simple_query(sql).await.err().map(|e| e.to_string());
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains(r#"column "ctid" does not exist"#)),
            "{sql} answered {error:?}"
        );
    }
    // Every row the policy would have hidden is still there.
    assert!(
        column(&engine, "SELECT a FROM guarded ORDER BY a").await
            == ["1", "2", "3"].map(|a| Some(a.to_string())).to_vec()
    );
}

/// A statement that names a system column of its target is reading that target,
/// and needs the `SELECT` privilege for it.
///
/// `PostgreSQL` marks a system column for `SELECT` like any other. Without the
/// rule a caller holding `DELETE` and not `SELECT` could aim at one row at a
/// time and read the row count back — an oracle over rows it may not see, and
/// one that destroys what it finds. It is reachable only because a `ctid`
/// resolves on this path at all.
#[tokio::test]
async fn naming_a_system_column_of_the_target_needs_the_select_privilege() {
    let engine = fixture().await;
    let mut owner = engine.connect();
    for sql in [
        "CREATE TABLE priv (a int, b text)",
        "INSERT INTO priv VALUES (1, 'one'), (2, 'two')",
        "CREATE ROLE writer",
        "GRANT INSERT, UPDATE, DELETE ON priv TO writer",
    ] {
        owner.simple_query(sql).await.expect("setup succeeds");
    }
    let mut writer = engine.connect();
    writer
        .simple_query("SET ROLE writer")
        .await
        .expect("set role");
    for sql in [
        "DELETE FROM priv WHERE ctid = '(0,1)'",
        "DELETE FROM priv WHERE priv.ctid = '(0,1)'",
        "UPDATE priv SET b = 'x' WHERE ctid = '(0,1)'",
        "DELETE FROM priv WHERE a = 1 RETURNING ctid",
        "UPDATE priv SET b = 'x' WHERE a = 1 RETURNING tableoid",
        "UPDATE priv SET b = 'x' RETURNING old.ctid",
    ] {
        let error = writer
            .simple_query(sql)
            .await
            .err()
            .map(|error| error.to_string());
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("permission denied")),
            "{sql} answered {error:?}"
        );
    }
    // The write that reads nothing of the target still runs, which is the rule
    // this one is an exception to and not a replacement for.
    writer
        .simple_query("DELETE FROM priv")
        .await
        .expect("a blind delete needs no read");
    assert!(column(&engine, "SELECT a FROM priv").await.is_empty());
}

/// The `OLD`/`NEW` images are the target under two more names, so reading one
/// needs the target's `SELECT` privilege.
///
/// This is not about system columns — `DELETE FROM t RETURNING old.secret`
/// handed every column of every row to a caller holding `DELETE` and no
/// `SELECT`. It is pinned beside them because `RETURNING old.ctid` reaches the
/// same gap, and because the `UPDATE` spelling was covered only by accident:
/// its `SET` list happened to read a column of its own.
#[tokio::test]
async fn an_image_alias_needs_the_targets_select_privilege_too() {
    let engine = fixture().await;
    let mut owner = engine.connect();
    for sql in [
        "CREATE TABLE priv (a int, secret text)",
        "INSERT INTO priv VALUES (1, 'hidden'), (2, 'also hidden')",
        "CREATE ROLE writer",
        "GRANT UPDATE, DELETE ON priv TO writer",
    ] {
        owner.simple_query(sql).await.expect("setup succeeds");
    }
    let mut writer = engine.connect();
    writer
        .simple_query("SET ROLE writer")
        .await
        .expect("set role");
    for sql in [
        "DELETE FROM priv RETURNING old.secret",
        "DELETE FROM priv RETURNING old.*",
        "DELETE FROM priv RETURNING old.ctid",
        "UPDATE priv SET a = 9 RETURNING old.secret",
        "UPDATE priv SET a = 9 RETURNING new.secret",
        "UPDATE priv SET a = 9 RETURNING WITH (OLD AS o) o.secret",
    ] {
        let error = writer
            .simple_query(sql)
            .await
            .err()
            .map(|error| error.to_string());
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("permission denied")),
            "{sql} answered {error:?}"
        );
    }
    assert!(
        column(&engine, "SELECT secret FROM priv ORDER BY a").await
            == vec![Some("hidden".to_string()), Some("also hidden".to_string())]
    );
}

/// An `OLD`/`NEW` reference is projected under the column's own name, whatever
/// is built around it.
///
/// A bare `old.v` has its output name pinned from the spelling the user wrote.
/// Anything wrapping one derives its name from the expression the image rewrite
/// left behind, which is an internal binding — so `RETURNING
/// old.tableoid::regclass` put a control character on the wire as a column
/// header. `returning.out` names that column `tableoid`.
#[tokio::test]
async fn an_image_reference_is_projected_under_its_own_column_name() {
    let engine = fixture().await;
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE foo (f1 int, f4 int)")
        .await
        .expect("setup");
    session
        .simple_query("INSERT INTO foo VALUES (1, 20)")
        .await
        .expect("setup");
    let cases = [
        (
            "UPDATE foo SET f4 = 100 WHERE f1 = 1 \
             RETURNING old.tableoid::regclass, old.ctid, old.*, new.ctid, new.*, *",
            vec![
                "tableoid", "ctid", "f1", "f4", "ctid", "f1", "f4", "f1", "f4",
            ],
        ),
        (
            "UPDATE foo SET f4 = 101 WHERE f1 = 1 \
             RETURNING old.f4::text||'->'||new.f4::text AS change",
            vec!["change"],
        ),
        // PostgreSQL's own name for an expression it cannot name.
        (
            "UPDATE foo SET f4 = 102 WHERE f1 = 1 RETURNING old.f4 + 1, new.f4",
            vec!["?column?", "f4"],
        ),
        (
            "DELETE FROM foo RETURNING old.tableoid::regclass, old.ctid",
            vec!["tableoid", "ctid"],
        ),
    ];
    for (sql, expected) in cases {
        let QueryResult::Rows { fields, .. } = session
            .simple_query(sql)
            .await
            .expect("statement succeeds")
            .pop()
            .expect("one result")
        else {
            panic!("expected rows from {sql}");
        };
        let names: Vec<String> = fields.into_iter().map(|field| field.name).collect();
        assert!(names == expected, "{sql} named {names:?}");
    }
}

#[tokio::test]
async fn ordinary_tuple_command_system_columns_come_from_its_header() {
    let engine = fixture().await;
    let mut session = engine.connect();
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t VALUES (3)")
        .await
        .expect("insert");
    let QueryResult::Rows { rows, .. } = session
        .simple_query("SELECT cmin::text, cmax::text, xmax::text FROM t WHERE a = 3")
        .await
        .expect("system-column scan")
        .pop()
        .expect("one result")
    else {
        panic!("expected rows");
    };
    let rendered = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.as_ref().map(text_of))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(rendered == vec![vec![Some("0".into()), Some("0".into()), Some("0".into())]]);
    session.simple_query("ROLLBACK").await.expect("rollback");
}

/// A data-modifying CTE shares the surrounding statement's command counter.
/// Its scan therefore sees the pre-update tuple, while the following command
/// sees the replacement.
#[tokio::test]
async fn data_modifying_cte_reads_the_pre_command_tuple_version() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE command_cte (a int)").await;
    let mut session = engine.connect();
    for sql in ["BEGIN", "INSERT INTO command_cte VALUES (1)"] {
        session.simple_query(sql).await.expect("setup succeeds");
    }
    let QueryResult::Rows { rows, .. } = session
        .simple_query(
            "WITH changed AS (UPDATE command_cte SET a = 2 RETURNING a) \
             SELECT a FROM command_cte",
        )
        .await
        .expect("CTE succeeds")
        .pop()
        .expect("one result")
    else {
        panic!("expected rows");
    };
    let rendered = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.as_ref().map(text_of))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(
        rendered == vec![vec![Some("1".to_string())]],
        "the main query must see the tuple as it stood before the CTE update"
    );
    let QueryResult::Rows { rows, .. } = session
        .simple_query("SELECT a FROM command_cte")
        .await
        .expect("following command succeeds")
        .pop()
        .expect("one result")
    else {
        panic!("expected rows");
    };
    let rendered = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.as_ref().map(text_of))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(rendered == vec![vec![Some("2".to_string())]]);
    session.simple_query("ROLLBACK").await.expect("rollback");
}
