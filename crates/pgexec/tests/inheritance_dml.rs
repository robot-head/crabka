//! `UPDATE` and `DELETE` against a table-inheritance parent.
//!
//! An unqualified write to a parent applies to every relation below it. The read
//! side has always done this, so a write that did not was not a missing feature
//! but a wrong answer: `DELETE FROM parent` reported rows removed and left them
//! readable through the same name.
//!
//! Every expectation here was taken from `PostgreSQL` 18.4 rather than derived
//! from the specification, because several are not obvious — an inheritance
//! `UPDATE` never relocates a row the way a partitioned one does, `RETURNING`
//! reports the whole tree in the *parent's* column shape, and both the ACL check
//! and the row-security policies come from the relation the statement named
//! rather than the one holding the row.

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

fn result_rows(result: &QueryResult, sql: &str) -> Vec<String> {
    match result {
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

async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    result_rows(&run(session, sql).await[0], sql)
}

async fn tag(session: &mut SqlSession, sql: &str) -> String {
    match &run(session, sql).await[0] {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
        other @ QueryResult::Empty => panic!("expected a tag from {sql}, got {other:?}"),
    }
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

async fn session_with(setup: &str) -> SqlSession {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, setup).await;
    session
}

/// A parent, a plain child, and a child carrying a column of its own.
const TREE: &str = r"
CREATE TABLE parent (a text, b int4);
CREATE TABLE child () INHERITS (parent);
CREATE TABLE wide (extra numeric) INHERITS (parent);
INSERT INTO parent VALUES ('P', 1);
INSERT INTO child  VALUES ('C', 2);
INSERT INTO wide   VALUES ('W', 3, 9.5);
";

/// A diamond: `d` inherits from both `b1` and `b2`, which both inherit `top`, so
/// `top` reaches `d` by two different routes.
const DIAMOND: &str = r"
CREATE TABLE top (a int4);
CREATE TABLE b1 () INHERITS (top);
CREATE TABLE b2 () INHERITS (top);
CREATE TABLE d () INHERITS (b1, b2);
INSERT INTO top VALUES (1);
INSERT INTO b1  VALUES (2);
INSERT INTO d   VALUES (3);
";

/// An unqualified write reaches the whole tree; `ONLY` stops at the parent.
#[tokio::test]
async fn only_decides_how_far_a_write_reaches() {
    let cases = [
        ("DELETE FROM parent", "DELETE 3", vec![]),
        ("DELETE FROM ONLY parent", "DELETE 1", vec!["C", "W"]),
        ("UPDATE parent SET a = a || '!'", "UPDATE 3", vec![]),
        ("UPDATE ONLY parent SET a = a || '!'", "UPDATE 1", vec![]),
    ];
    for (sql, expected_tag, survivors) in cases {
        let mut session = session_with(TREE).await;
        assert!(tag(&mut session, sql).await == expected_tag, "{sql}");
        if sql.starts_with("DELETE") {
            let left = query(&mut session, "SELECT a FROM parent ORDER BY a").await;
            assert!(left == rows(&survivors), "{sql} left {left:?}");
        }
    }
}

/// An `UPDATE` through the parent changes the children's rows, in place.
///
/// `PostgreSQL` never relocates an inheritance row — unlike a partitioned
/// `UPDATE`, a new value that would suit the parent does not move the row there.
#[tokio::test]
async fn update_changes_child_rows_where_they_live() {
    let mut session = session_with(TREE).await;
    assert!(tag(&mut session, "UPDATE parent SET b = b + 100").await == "UPDATE 3");
    assert!(
        query(&mut session, "SELECT a, b FROM parent ORDER BY a").await
            == rows(&["C,102", "P,101", "W,103"])
    );
    // Each row is still in the relation that held it.
    assert!(query(&mut session, "SELECT a FROM ONLY parent").await == rows(&["P"]));
    assert!(query(&mut session, "SELECT a FROM child").await == rows(&["C"]));
    assert!(query(&mut session, "SELECT a, extra FROM wide").await == rows(&["W,9.5"]));
}

/// A relation reachable by two routes is written once, not once per route.
#[tokio::test]
async fn a_diamond_writes_each_relation_once() {
    let mut session = session_with(DIAMOND).await;
    assert!(tag(&mut session, "UPDATE top SET a = a + 10").await == "UPDATE 3");
    assert!(query(&mut session, "SELECT a FROM top ORDER BY a").await == rows(&["11", "12", "13"]));
    assert!(tag(&mut session, "DELETE FROM top").await == "DELETE 3");
    assert!(query(&mut session, "SELECT a FROM top").await == rows(&[]));
}

/// `RETURNING *` reports the tree in the parent's column shape.
///
/// `wide` stores a third column; `PostgreSQL` reports the parent's two and drops
/// it, so the result of one statement has one shape however many relations
/// contributed to it.
#[tokio::test]
async fn returning_reports_the_parents_column_shape() {
    let mut session = session_with(TREE).await;
    let updated = &run(&mut session, "UPDATE parent SET b = b + 1 RETURNING *").await[0];
    let QueryResult::Rows { fields, .. } = updated else {
        panic!("expected rows")
    };
    let names: Vec<String> = fields.iter().map(|field| field.name.clone()).collect();
    assert!(names == rows(&["a", "b"]), "got {names:?}");
    let mut got = result_rows(updated, "UPDATE … RETURNING *");
    got.sort();
    assert!(got == rows(&["C,3", "P,2", "W,4"]), "got {got:?}");

    let deleted = &run(&mut session, "DELETE FROM parent RETURNING *").await[0];
    let mut got = result_rows(deleted, "DELETE … RETURNING *");
    got.sort();
    assert!(got == rows(&["C,3", "P,2", "W,4"]), "got {got:?}");
}

/// A reference qualified by the name the statement was written against keeps
/// resolving once the write is retargeted at a child.
///
/// The target's qualifier defaults to its own table name, so retargeting used to
/// move it from `parent` to `child` and `WHERE parent.b = 1` stopped resolving.
/// The same fault reached partitioned writes, which have no inheritance in them
/// at all — see `partitioned_dml_keeps_the_parents_qualifier`.
#[tokio::test]
async fn a_parent_qualified_reference_still_resolves() {
    let mut session = session_with(TREE).await;
    assert!(
        tag(
            &mut session,
            "UPDATE parent SET b = parent.b + 1 WHERE parent.b > 1"
        )
        .await
            == "UPDATE 2"
    );
    assert!(tag(&mut session, "DELETE FROM parent WHERE parent.a = 'C'").await == "DELETE 1");
    // An explicit alias replaces the table name, exactly as it does for a
    // single-relation write.
    assert!(tag(&mut session, "UPDATE parent AS z SET b = z.b WHERE z.b > 0").await == "UPDATE 2");
    assert!(
        query(
            &mut session,
            "UPDATE parent AS z SET b = 7 RETURNING z.a, z.b"
        )
        .await
        .len()
            == 2
    );
}

/// The same qualifier fault on a partitioned table, which has no inheritance in
/// it: this is a regression test for a defect that predates the tree write.
#[tokio::test]
async fn partitioned_dml_keeps_the_parents_qualifier() {
    let mut session = session_with(
        r"
CREATE TABLE part (x int4, y text) PARTITION BY RANGE (x);
CREATE TABLE part1 PARTITION OF part FOR VALUES FROM (0) TO (100);
INSERT INTO part VALUES (1, 'a'), (2, 'b');
",
    )
    .await;
    assert!(tag(&mut session, "UPDATE part SET y = 'z' WHERE part.x = 1").await == "UPDATE 1");
    assert!(tag(&mut session, "UPDATE part SET y = part.y WHERE x = 2").await == "UPDATE 1");
    assert!(
        query(&mut session, "UPDATE part SET y = y RETURNING part.x")
            .await
            .len()
            == 2
    );
    assert!(tag(&mut session, "DELETE FROM part WHERE part.x = 1").await == "DELETE 1");
}

/// `TRUNCATE` follows the same rule as the other two writes.
#[tokio::test]
async fn truncate_empties_the_tree_unless_only() {
    let mut session = session_with(TREE).await;
    run(&mut session, "TRUNCATE ONLY parent").await;
    assert!(query(&mut session, "SELECT a FROM parent ORDER BY a").await == rows(&["C", "W"]));
    run(&mut session, "TRUNCATE parent").await;
    assert!(query(&mut session, "SELECT a FROM parent").await == rows(&[]));
}

/// The named parent's ACL authorizes the whole tree.
///
/// `PostgreSQL` checks the relation the statement named and none of its
/// descendants', so a role holding `UPDATE`/`DELETE` on the parent alone still
/// writes the children's rows. Resolving the check against the relation holding
/// the row instead refuses writes `PostgreSQL` allows.
#[tokio::test]
async fn the_named_parents_privileges_authorize_the_tree() {
    let mut session = session_with(
        r"
CREATE ROLE writer;
CREATE TABLE parent (a text, b int4);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES ('P', 1);
INSERT INTO child  VALUES ('C', 2);
GRANT SELECT, UPDATE, DELETE ON parent TO writer;
",
    )
    .await;
    run(&mut session, "SET ROLE writer").await;
    assert!(tag(&mut session, "UPDATE parent SET b = b + 1").await == "UPDATE 2");
    assert!(tag(&mut session, "DELETE FROM parent").await == "DELETE 2");
}

/// The same rule for a partitioned parent, which has no inheritance in it: a
/// grant on the partitioned table alone authorizes the leaf writes. This is a
/// regression test for a defect that predates the tree write.
#[tokio::test]
async fn a_partitioned_parents_privileges_authorize_its_leaves() {
    let mut session = session_with(
        r"
CREATE ROLE writer;
CREATE TABLE part (x int4, y text) PARTITION BY RANGE (x);
CREATE TABLE part1 PARTITION OF part FOR VALUES FROM (0) TO (100);
INSERT INTO part VALUES (1, 'a');
GRANT SELECT, UPDATE, DELETE ON part TO writer;
",
    )
    .await;
    run(&mut session, "SET ROLE writer").await;
    assert!(tag(&mut session, "UPDATE part SET y = 'z'").await == "UPDATE 1");
    assert!(tag(&mut session, "DELETE FROM part").await == "DELETE 1");
}

/// The named parent's row-security policies filter the children's rows, even
/// when a child has row security disabled and declares no policy of its own.
#[tokio::test]
async fn the_named_parents_policies_govern_child_rows() {
    let mut session = session_with(
        r"
CREATE ROLE writer;
CREATE TABLE parent (a int4, tag text);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES (1, 'keep'), (2, 'hide');
INSERT INTO child  VALUES (3, 'keep'), (4, 'hide');
GRANT SELECT, UPDATE, DELETE ON parent, child TO writer;
ALTER TABLE parent ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible ON parent USING (tag = 'keep');
",
    )
    .await;
    run(&mut session, "SET ROLE writer").await;
    assert!(tag(&mut session, "DELETE FROM parent").await == "DELETE 2");
    run(&mut session, "RESET ROLE").await;
    assert!(
        query(&mut session, "SELECT a, tag FROM parent ORDER BY a").await
            == rows(&["2,hide", "4,hide"])
    );
}

/// A statement trigger on the parent sees the children's rows in its transition
/// table, reshaped into the parent's column list.
///
/// `wide` stores a third column of its own; the parent's `OLD TABLE` shows the
/// parent's two and drops it.
#[tokio::test]
async fn a_transition_table_shows_child_rows_in_parent_shape() {
    let mut session = session_with(
        r"
CREATE TABLE parent (a text, b int4);
CREATE TABLE child () INHERITS (parent);
CREATE TABLE wide (extra numeric) INHERITS (parent);
CREATE TABLE seen (a text, b int4);
CREATE FUNCTION capture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO seen SELECT a, b FROM oldt; RETURN NULL; END $$;
CREATE TRIGGER watch AFTER DELETE ON parent
  REFERENCING OLD TABLE AS oldt FOR EACH STATEMENT EXECUTE FUNCTION capture();
INSERT INTO parent VALUES ('P', 1);
INSERT INTO child  VALUES ('C', 2);
INSERT INTO wide   VALUES ('W', 3, 9.5);
",
    )
    .await;
    assert!(tag(&mut session, "DELETE FROM parent").await == "DELETE 3");
    assert!(
        query(&mut session, "SELECT a, b FROM seen ORDER BY a").await
            == rows(&["C,2", "P,1", "W,3"])
    );
}

/// A write to a relation with no children behaves exactly as before, and a
/// `RETURNING *` over it still reports that relation's own columns.
#[tokio::test]
async fn a_childless_write_is_unchanged() {
    let mut session = session_with(
        r"
CREATE TABLE solo (a int4, b text);
INSERT INTO solo VALUES (1, 'x'), (2, 'y');
",
    )
    .await;
    assert!(tag(&mut session, "UPDATE solo SET a = a * 2 RETURNING *").await == "UPDATE 2");
    assert!(
        query(
            &mut session,
            "DELETE FROM solo WHERE solo.a = 4 RETURNING *"
        )
        .await
            == rows(&["4,y"])
    );
    assert!(query(&mut session, "SELECT a, b FROM solo").await == rows(&["2,x"]));
}
