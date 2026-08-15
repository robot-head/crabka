//! `UPDATE ONLY`, `DELETE FROM ONLY` and `TRUNCATE ONLY`.
//!
//! `ONLY` restricts a statement to the named relation instead of its whole
//! inheritance tree, and the two spellings genuinely differ: without it all
//! three commands descend into every relation below the target.
//!
//! `ONLY` also has to *parse*, and it used to not: `only` was taken as the table
//! name and the real table became its alias, which surfaced as `relation "only"
//! does not exist`.
//!
//! A *partitioned* parent is the other half, and the flag used to be dropped on
//! the floor for one: the engine asked whether the relation was partitioned
//! before it asked what the statement wanted, so every partitioned read and
//! write expanded into the leaves whatever the statement said. `SELECT * FROM
//! ONLY parted` returned the leaves' rows where `PostgreSQL` returns none, and
//! `DELETE FROM ONLY parted` destroyed them — a statement whose entire purpose
//! is to spare the children.
//!
//! The `ONLY` family is not uniform over a partitioned parent, so the cases
//! below are enumerated rather than derived. `SELECT`, `UPDATE` and `DELETE`
//! read or write its own — empty — row space and report nothing done;
//! `TRUNCATE` refuses with 42809; `INSERT` has no `ONLY` to say. All of it was
//! measured against `PostgreSQL` 18.4.
//!
//! Every partitioned case here is paired with the same statement without
//! `ONLY`. The fix works by *declining* to expand, so a one-sided test would
//! pass just as well against an engine that had lost the expansion altogether —
//! and losing it is the live hazard, because `TRUNCATE parted` desugars to an
//! unfiltered `DELETE` that carries the very flag `ONLY` sets.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, QueryResult, Session},
    error::PgError,
};

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
    match &run(session, sql).await[0] {
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

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A parent with one inheriting child, each holding one row.
const SETUP: &str = r"
CREATE TABLE parent (id int4, tag text);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES (1, 'p');
INSERT INTO child VALUES (2, 'c');
";

async fn tree() -> SqlSession {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, SETUP).await;
    session
}

/// `ONLY` names the relation it precedes, on every command that takes it, in
/// both the bare and the schema-qualified spelling.
#[tokio::test]
async fn only_names_the_relation_it_precedes() {
    let cases = [
        "UPDATE ONLY parent SET tag = 'x'",
        "UPDATE ONLY public.parent SET tag = 'x'",
        "UPDATE ONLY parent AS p SET tag = 'x' WHERE p.id = 1",
        "DELETE FROM ONLY parent",
        "DELETE FROM ONLY public.parent WHERE id = 1",
        "TRUNCATE ONLY parent",
        "TRUNCATE TABLE ONLY public.parent",
    ];
    for sql in cases {
        let mut session = tree().await;
        run(&mut session, sql).await;
    }
}

/// `UPDATE ONLY` writes the parent's own rows and leaves the child's alone.
#[tokio::test]
async fn update_only_touches_the_named_relation() {
    let mut session = tree().await;
    run(&mut session, "UPDATE ONLY parent SET tag = 'x'").await;
    assert!(
        query(&mut session, "SELECT id, tag FROM ONLY parent ORDER BY id").await == rows(&["1,x"])
    );
    assert!(query(&mut session, "SELECT id, tag FROM child").await == rows(&["2,c"]));
}

/// `DELETE FROM ONLY` likewise.
#[tokio::test]
async fn delete_only_touches_the_named_relation() {
    let mut session = tree().await;
    run(&mut session, "DELETE FROM ONLY parent").await;
    assert!(query(&mut session, "SELECT id FROM ONLY parent").await == rows(&[]));
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&["2"]));
}

/// `TRUNCATE ONLY` likewise, and `ONLY` binds to one name in a list rather than
/// to the whole list.
#[tokio::test]
async fn truncate_only_binds_per_name() {
    let mut session = tree().await;
    run(&mut session, "CREATE TABLE other (id int4)").await;
    run(&mut session, "INSERT INTO other VALUES (9)").await;
    run(&mut session, "TRUNCATE ONLY parent, other").await;
    assert!(query(&mut session, "SELECT id FROM ONLY parent").await == rows(&[]));
    assert!(query(&mut session, "SELECT id FROM other").await == rows(&[]));
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&["2"]));
}

/// Omitting `ONLY` reaches the children, on all three commands.
///
/// This is the case that made the flag worth honouring: `SELECT count(*) FROM
/// parent` has always counted the child's row, so a `DELETE FROM parent` that
/// walked past it left the hierarchy holding rows the same statement claimed to
/// have removed.
#[tokio::test]
async fn omitting_only_reaches_the_children() {
    let mut session = tree().await;
    run(&mut session, "UPDATE parent SET tag = 'x'").await;
    assert!(query(&mut session, "SELECT tag FROM child").await == rows(&["x"]));

    run(&mut session, "DELETE FROM parent").await;
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&[]));

    run(&mut session, "INSERT INTO parent VALUES (1, 'p')").await;
    run(&mut session, "INSERT INTO child VALUES (2, 'c')").await;
    run(&mut session, "TRUNCATE parent").await;
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&[]));
}

/// The command tag counts every row the statement touched, across the tree.
#[tokio::test]
async fn the_command_tag_counts_the_whole_tree() {
    let cases = [
        ("UPDATE parent SET tag = 'x'", "UPDATE 2"),
        ("UPDATE ONLY parent SET tag = 'x'", "UPDATE 1"),
        ("DELETE FROM parent", "DELETE 2"),
        ("DELETE FROM ONLY parent", "DELETE 1"),
    ];
    for (sql, expected) in cases {
        let mut session = tree().await;
        let tag = match &run(&mut session, sql).await[0] {
            QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
            other @ QueryResult::Empty => panic!("expected a tag from {sql}, got {other:?}"),
        };
        assert!(tag == expected, "{sql} reported {tag}, expected {expected}");
    }
}

/// A table called `only` is reachable, and quoting is how: `ONLY` is a
/// `reserved_keyword`, so the bare word is always the modifier and never a name.
///
/// `PostgreSQL` 18.4 refuses every bare spelling below, and where it puts the
/// caret says why: on the `UPDATE` it points at the `=` rather than at `only`,
/// because it read the modifier and then wanted the relation that never came.
/// This parser used to read a lone `only` as a relation of that name, which is
/// the same under-refusal as `CREATE TABLE t (check int)` — a word reached a
/// name position as a plain identifier and nothing asked whether `PostgreSQL`
/// reserves it.
#[tokio::test]
async fn a_table_called_only_needs_its_quotes() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for sql in [
        "CREATE TABLE only (id int4)",
        "INSERT INTO only VALUES (1)",
        "UPDATE only SET id = 2",
        "SELECT id FROM only",
        "DELETE FROM only",
        "TRUNCATE only",
    ] {
        let error = outcome(&mut session, sql)
            .await
            .expect_err("`only` is reserved, so it is not a name");
        assert!(error.code == "42601", "{sql} reported {}", error.code);
    }

    // Quoted, the same six statements are the ones 18.4 accepts, and the table
    // round-trips through all of them.
    run(&mut session, r#"CREATE TABLE "only" (id int4)"#).await;
    run(&mut session, r#"INSERT INTO "only" VALUES (1)"#).await;
    run(&mut session, r#"UPDATE "only" SET id = 2"#).await;
    assert!(query(&mut session, r#"SELECT id FROM "only""#).await == rows(&["2"]));
    run(&mut session, r#"DELETE FROM "only""#).await;
    assert!(query(&mut session, r#"SELECT id FROM "only""#).await == rows(&[]));
    run(&mut session, r#"TRUNCATE "only""#).await;
}

// ── A partitioned parent ─────────────────────────────────────────────────────

/// A range-partitioned parent with two leaves, each holding two rows. The
/// parent itself stores none, which is the whole point: `ONLY` over it names a
/// row space that is empty by construction.
const PARTITIONED: &str = r"
CREATE TABLE parted (a int4, tag text) PARTITION BY RANGE (a);
CREATE TABLE parted_low PARTITION OF parted FOR VALUES FROM (0) TO (10);
CREATE TABLE parted_high PARTITION OF parted FOR VALUES FROM (10) TO (20);
INSERT INTO parted VALUES (1, 'l1'), (2, 'l2'), (11, 'h1'), (12, 'h2');
";

async fn partitioned() -> SqlSession {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, PARTITIONED).await;
    session
}

/// Rows of each leaf, read through the leaf itself so the parent's own scan
/// path cannot mask a leaf the statement emptied.
async fn leaves(session: &mut SqlSession) -> (Vec<String>, Vec<String>) {
    (
        query(session, "SELECT tag FROM parted_low ORDER BY a").await,
        query(session, "SELECT tag FROM parted_high ORDER BY a").await,
    )
}

/// A statement's outcome as either its command tag or its `SQLSTATE`.
async fn outcome(session: &mut SqlSession, sql: &str) -> Result<String, PgError> {
    match session.simple_query(sql).await {
        Ok(results) => Ok(match &results[0] {
            QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
            QueryResult::Empty => String::new(),
        }),
        Err(error) => Err(error),
    }
}

/// `SELECT … FROM ONLY parted` reads the parent's own row space, which is
/// empty; the same read without `ONLY` returns the leaves' rows.
#[tokio::test]
async fn select_only_a_partitioned_parent_reads_no_rows() {
    let mut session = partitioned().await;
    assert!(query(&mut session, "SELECT tag FROM ONLY parted ORDER BY a").await == rows(&[]));
    assert!(
        query(&mut session, "SELECT tag FROM parted ORDER BY a").await
            == rows(&["l1", "l2", "h1", "h2"])
    );
    // The aggregate pushdowns fold over one relation's row space and had the
    // same hole, so they are asked in the same breath rather than trusted to
    // share the general path's answer.
    assert!(query(&mut session, "SELECT count(*) FROM ONLY parted").await == rows(&["0"]));
    assert!(query(&mut session, "SELECT count(*) FROM parted").await == rows(&["4"]));
    assert!(query(&mut session, "SELECT max(a) FROM ONLY parted").await == rows(&["NULL"]));
    assert!(query(&mut session, "SELECT max(a) FROM parted").await == rows(&["12"]));
}

/// `DELETE FROM ONLY parted` deletes nothing and every leaf row survives.
///
/// The regression this file exists for. The statement used to route straight
/// into the per-leaf walk and empty the whole hierarchy, reporting `DELETE 4`
/// for a statement `PostgreSQL` answers `DELETE 0` — unrecoverable data loss
/// from the one spelling that promises to leave the children alone.
#[tokio::test]
async fn delete_only_a_partitioned_parent_spares_every_leaf() {
    let mut session = partitioned().await;
    assert!(outcome(&mut session, "DELETE FROM ONLY parted").await == Ok("DELETE 0".into()));
    assert!(leaves(&mut session).await == (rows(&["l1", "l2"]), rows(&["h1", "h2"])));

    // The witness: without `ONLY` the same statement still reaches both leaves.
    assert!(outcome(&mut session, "DELETE FROM parted").await == Ok("DELETE 4".into()));
    assert!(leaves(&mut session).await == (rows(&[]), rows(&[])));
}

/// `UPDATE ONLY parted` updates nothing and leaves every leaf row as it was.
#[tokio::test]
async fn update_only_a_partitioned_parent_changes_no_leaf() {
    let mut session = partitioned().await;
    assert!(
        outcome(&mut session, "UPDATE ONLY parted SET tag = 'x'").await == Ok("UPDATE 0".into())
    );
    assert!(leaves(&mut session).await == (rows(&["l1", "l2"]), rows(&["h1", "h2"])));

    assert!(outcome(&mut session, "UPDATE parted SET tag = 'x'").await == Ok("UPDATE 4".into()));
    assert!(leaves(&mut session).await == (rows(&["x", "x"]), rows(&["x", "x"])));
}

/// `TRUNCATE ONLY parted` is 42809, not a no-op and not a truncation.
///
/// The one member of the family that refuses. `PostgreSQL` takes the view that
/// a partitioned parent has no storage to truncate, so naming it under `ONLY`
/// is a mistake rather than a request for nothing.
#[tokio::test]
async fn truncate_only_a_partitioned_parent_is_refused() {
    let mut session = partitioned().await;
    let refused = outcome(&mut session, "TRUNCATE ONLY parted")
        .await
        .expect_err("TRUNCATE ONLY over a partitioned parent is refused");
    assert!(refused.code == "42809");
    assert!(refused.message == "cannot truncate only a partitioned table");
    assert!(
        refused.diagnostics.and_then(|diagnostics| diagnostics.hint)
            == Some(
                "Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions \
                 directly."
                    .into()
            )
    );
    // Refused before anything was emptied, and all-or-nothing across the list.
    assert!(leaves(&mut session).await == (rows(&["l1", "l2"]), rows(&["h1", "h2"])));

    // The witness, and the case a one-line fix breaks: `TRUNCATE` desugars to
    // an unfiltered `DELETE` that says `ONLY` to stop the inheritance walk it
    // has already done. Read as the user's `ONLY`, it empties nothing.
    assert!(outcome(&mut session, "TRUNCATE parted").await == Ok("TRUNCATE TABLE".into()));
    assert!(leaves(&mut session).await == (rows(&[]), rows(&[])));
}

/// A locking read of `ONLY` a partitioned parent returns no rows rather than
/// refusing: there is no lock to spread over leaves that contribute nothing.
#[tokio::test]
async fn locking_read_of_only_a_partitioned_parent_returns_no_rows() {
    let mut session = partitioned().await;
    assert!(query(&mut session, "SELECT tag FROM ONLY parted FOR UPDATE").await == rows(&[]));
    // Still refused where the lock really would have to reach the leaves.
    let refused = outcome(&mut session, "SELECT tag FROM parted FOR UPDATE")
        .await
        .expect_err("a locking read of the whole hierarchy is refused");
    assert!(refused.code == "0A000");
}

/// `ONLY` over a *leaf* names a relation that does hold rows, and every command
/// writes it. The parent's emptiness is a property of being partitioned, not of
/// the keyword.
#[tokio::test]
async fn only_a_leaf_partition_writes_that_leaf() {
    let mut session = partitioned().await;
    assert!(
        query(&mut session, "SELECT tag FROM ONLY parted_low ORDER BY a").await
            == rows(&["l1", "l2"])
    );
    assert!(
        outcome(&mut session, "UPDATE ONLY parted_low SET tag = 'x'").await
            == Ok("UPDATE 2".into())
    );
    assert!(outcome(&mut session, "DELETE FROM ONLY parted_high").await == Ok("DELETE 2".into()));
    assert!(leaves(&mut session).await == (rows(&["x", "x"]), rows(&[])));
    run(&mut session, "TRUNCATE ONLY parted_low").await;
    assert!(leaves(&mut session).await == (rows(&[]), rows(&[])));
}

/// An intermediate partitioned relation answers like any other partitioned
/// parent: `ONLY` sees its own empty storage, and the walk below it still
/// reaches the sub-leaf that holds the rows.
#[tokio::test]
async fn only_an_intermediate_partitioned_relation_reads_no_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        r"
CREATE TABLE top (a int4, b int4) PARTITION BY RANGE (a);
CREATE TABLE mid PARTITION OF top FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (b);
CREATE TABLE leaf PARTITION OF mid FOR VALUES FROM (0) TO (10);
INSERT INTO top VALUES (1, 1), (2, 2);
",
    )
    .await;
    assert!(query(&mut session, "SELECT count(*) FROM ONLY top").await == rows(&["0"]));
    assert!(query(&mut session, "SELECT count(*) FROM ONLY mid").await == rows(&["0"]));
    assert!(query(&mut session, "SELECT count(*) FROM mid").await == rows(&["2"]));
    assert!(query(&mut session, "SELECT count(*) FROM top").await == rows(&["2"]));

    assert!(outcome(&mut session, "DELETE FROM ONLY mid").await == Ok("DELETE 0".into()));
    assert!(query(&mut session, "SELECT count(*) FROM leaf").await == rows(&["2"]));
    // The witness reaches two levels down, so a walk that stopped at `mid`
    // would fail here rather than pass quietly.
    run(&mut session, "TRUNCATE top").await;
    assert!(query(&mut session, "SELECT count(*) FROM leaf").await == rows(&["0"]));
}

/// A view over a partitioned table is rewritten onto the base relation, and
/// that rewrite pins the same flag `ONLY` sets. It must still reach the leaves.
#[tokio::test]
async fn writing_through_a_view_still_reaches_the_partitions() {
    let mut session = partitioned().await;
    run(&mut session, "CREATE VIEW parted_v AS SELECT * FROM parted").await;
    assert!(query(&mut session, "SELECT count(*) FROM parted_v").await == rows(&["4"]));
    assert!(outcome(&mut session, "UPDATE parted_v SET tag = 'v'").await == Ok("UPDATE 4".into()));
    assert!(leaves(&mut session).await == (rows(&["v", "v"]), rows(&["v", "v"])));
    assert!(outcome(&mut session, "DELETE FROM parted_v").await == Ok("DELETE 4".into()));
    assert!(leaves(&mut session).await == (rows(&[]), rows(&[])));
}

/// A data-modifying CTE carries `ONLY` the same way a bare statement does.
#[tokio::test]
async fn only_inside_a_data_modifying_cte_spares_every_leaf() {
    let mut session = partitioned().await;
    assert!(
        query(
            &mut session,
            "WITH gone AS (DELETE FROM ONLY parted RETURNING a) SELECT count(*) FROM gone"
        )
        .await
            == rows(&["0"])
    );
    assert!(leaves(&mut session).await == (rows(&["l1", "l2"]), rows(&["h1", "h2"])));

    assert!(
        query(
            &mut session,
            "WITH gone AS (DELETE FROM parted RETURNING a) SELECT count(*) FROM gone"
        )
        .await
            == rows(&["4"])
    );
    assert!(leaves(&mut session).await == (rows(&[]), rows(&[])));
}
