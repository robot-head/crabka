//! `PostgreSQL` 17's table-constraint spelling of a not-null — `[CONSTRAINT n]
//! NOT NULL <column>` — and the `ALTER TABLE … ALTER CONSTRAINT` subcommand
//! that writes a constraint's properties in place.
//!
//! Crabka stores a not-null as a flag on the column, so the table-level
//! spelling is the column-level one under another name: the constraint is
//! enforced on every write, recurses to descendants, and shows up in
//! `pg_constraint` with `contype = 'n'`. The two attributes the flag cannot hold
//! — `NOT VALID` and `NO INHERIT` — are refused rather than dropped.
//!
//! Every expectation is the behaviour of a live `PostgreSQL` 18.4, except where
//! a comment names the refusal Crabka substitutes.

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

async fn err_code(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql).await.expect_err("expected error").code
}

async fn err_message(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql)
        .await
        .expect_err("expected error")
        .message
}

async fn err_detail(s: &mut SqlSession, sql: &str) -> Option<String> {
    s.simple_query(sql)
        .await
        .expect_err("expected error")
        .diagnostics
        .and_then(|fields| fields.detail)
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

// Whether the column is not-null, read back the way a client would: the
// `pg_constraint` row PostgreSQL records for it.
async fn not_null_constraints(s: &mut SqlSession, table: &str) -> Vec<Vec<Option<String>>> {
    query(
        s,
        &format!(
            "SELECT conname FROM pg_constraint \
             WHERE conrelid = '{table}'::regclass AND contype = 'n' ORDER BY conname"
        ),
    )
    .await
}

// Each table-level spelling of a not-null puts the same flag on the column as
// `a int NOT NULL` does, whether or not the constraint is labelled and wherever
// in the element list it sits.
#[tokio::test]
async fn every_table_level_spelling_makes_the_column_not_null() {
    let definitions = [
        "(a int, CONSTRAINT c NOT NULL a)",
        "(a int, NOT NULL a)",
        "(CONSTRAINT c NOT NULL a, a int)",
        "(a int, b int, NOT NULL a)",
    ];
    for definition in definitions {
        let (_engine, mut s) = engine_with(&[&format!("CREATE TABLE t {definition}")]).await;
        assert!(
            not_null_constraints(&mut s, "t").await == vec![text_row(&["t_a_not_null"])],
            "{definition}"
        );
        assert!(
            err_code(&mut s, "INSERT INTO t (a) VALUES (NULL)").await == "23502",
            "{definition}"
        );
        run(&mut s, "INSERT INTO t (a) VALUES (1)").await;
    }
}

#[tokio::test]
async fn write_constraint_errors_include_the_failing_row() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE nn (a int NOT NULL, b text)",
        "CREATE TABLE chk (a int CHECK (a > 0))",
    ])
    .await;
    assert!(
        err_detail(&mut s, "INSERT INTO nn VALUES (NULL, 'text')").await
            == Some("Failing row contains (null, text).".into())
    );
    assert!(
        err_detail(&mut s, "INSERT INTO chk VALUES (0)").await
            == Some("Failing row contains (0).".into())
    );
}

// `ADD [CONSTRAINT n] NOT NULL c` is `ALTER COLUMN c SET NOT NULL` in another
// spelling, down to scanning the rows already stored before it takes.
#[tokio::test]
async fn add_not_null_scans_the_rows_already_stored() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (a int, b int)",
        "INSERT INTO t VALUES (NULL, 1)",
    ])
    .await;
    assert!(
        err_message(&mut s, "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a").await
            == "column \"a\" of relation \"t\" contains null values"
    );
    assert!(not_null_constraints(&mut s, "t").await.is_empty());

    run(&mut s, "ALTER TABLE t ADD CONSTRAINT nn NOT NULL b").await;
    assert!(not_null_constraints(&mut s, "t").await == vec![text_row(&["t_b_not_null"])]);
    assert!(err_code(&mut s, "INSERT INTO t (a, b) VALUES (1, NULL)").await == "23502");
}

// The unnamed `ADD NOT NULL c` is the same subcommand.
#[tokio::test]
async fn the_unnamed_add_not_null_spelling_works_too() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    run(&mut s, "ALTER TABLE t ADD NOT NULL a").await;
    assert!(not_null_constraints(&mut s, "t").await == vec![text_row(&["t_a_not_null"])]);
    assert!(err_code(&mut s, "INSERT INTO t VALUES (NULL)").await == "23502");
}

// Like `SET NOT NULL`, the table-level spelling recurses into descendants, and
// a descendant holding a null stops the whole statement.
#[tokio::test]
async fn add_not_null_recurses_into_descendants() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE parent (a int)",
        "CREATE TABLE child () INHERITS (parent)",
        "INSERT INTO child VALUES (NULL)",
    ])
    .await;
    assert!(
        err_message(&mut s, "ALTER TABLE parent ADD CONSTRAINT nn NOT NULL a").await
            == "column \"a\" of relation \"child\" contains null values"
    );
    run(&mut s, "DELETE FROM child").await;
    run(&mut s, "ALTER TABLE parent ADD CONSTRAINT nn NOT NULL a").await;
    assert!(not_null_constraints(&mut s, "child").await == vec![text_row(&["child_a_not_null"])]);
    assert!(err_code(&mut s, "INSERT INTO child VALUES (NULL)").await == "23502");
}

// A column flag holds neither an unvalidated constraint nor one a child does
// not get, so both attributes are refused instead of accepted and dropped.
#[tokio::test]
async fn the_attributes_a_column_flag_cannot_hold_are_refused() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    for sql in [
        "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a NOT VALID",
        "ALTER TABLE t ADD NOT NULL a NO INHERIT",
        "CREATE TABLE u (a int, CONSTRAINT nn NOT NULL a NO INHERIT)",
        "CREATE TABLE u (a int, NOT NULL a NO INHERIT)",
    ] {
        assert!(err_code(&mut s, sql).await == "0A000", "{sql}");
    }
    // The refusals leave nothing behind.
    assert!(not_null_constraints(&mut s, "t").await.is_empty());
    assert!(err_code(&mut s, "SELECT * FROM u").await == "42P01");
}

// A table-level not-null that names no column of the relation is the same
// 42703 an `ALTER COLUMN` on a missing column reports.
#[tokio::test]
async fn a_table_level_not_null_must_name_a_column_the_relation_has() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    assert!(err_code(&mut s, "CREATE TABLE u (a int, NOT NULL b)").await == "42703");
    assert!(err_code(&mut s, "ALTER TABLE t ADD CONSTRAINT nn NOT NULL b").await == "42703");
}

// `NOT NULL <column>` may name a column the statement inherits rather than one
// it declares, which is the whole reason it is applied after the merge.
#[tokio::test]
async fn a_table_level_not_null_reaches_an_inherited_column() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE parent (a int)",
        "CREATE TABLE child (CONSTRAINT c NOT NULL a) INHERITS (parent)",
    ])
    .await;
    assert!(not_null_constraints(&mut s, "child").await == vec![text_row(&["child_a_not_null"])]);
    assert!(err_code(&mut s, "INSERT INTO child VALUES (NULL)").await == "23502");
    // The parent keeps its own nullable column.
    assert!(not_null_constraints(&mut s, "parent").await.is_empty());
    run(&mut s, "INSERT INTO parent VALUES (NULL)").await;
}

// `ALTER CONSTRAINT` on a name the relation does not carry is PostgreSQL's
// 42704, worded the way that subcommand words it.
#[tokio::test]
async fn alter_constraint_reports_a_name_the_relation_does_not_carry() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    assert!(
        err_message(&mut s, "ALTER TABLE t ALTER CONSTRAINT nope NO INHERIT").await
            == "constraint \"nope\" of relation \"t\" does not exist"
    );
    assert!(err_code(&mut s, "ALTER TABLE t ALTER CONSTRAINT nope NO INHERIT").await == "42704");
}

// PostgreSQL admits a deferrability or enforceability change on a foreign key
// alone, and an inheritability change on a not-null alone. Every other pairing
// is a 42809 naming the constraint and the relation.
#[tokio::test]
async fn alter_constraint_refuses_a_property_the_constraint_kind_does_not_have() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int PRIMARY KEY)",
        "CREATE TABLE t (a int UNIQUE, b int CHECK (b > 0), c int REFERENCES p (id))",
        "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a",
    ])
    .await;
    let cases = [
        (
            "ALTER TABLE t ALTER CONSTRAINT t_a_key DEFERRABLE",
            "constraint \"t_a_key\" of relation \"t\" is not a foreign key constraint",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT t_b_check DEFERRABLE INITIALLY DEFERRED",
            "constraint \"t_b_check\" of relation \"t\" is not a foreign key constraint",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT t_a_not_null NOT DEFERRABLE",
            "constraint \"t_a_not_null\" of relation \"t\" is not a foreign key constraint",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT t_a_key ENFORCED",
            "cannot alter enforceability of constraint \"t_a_key\" of relation \"t\"",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT t_b_check NOT ENFORCED",
            "cannot alter enforceability of constraint \"t_b_check\" of relation \"t\"",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT t_c_fkey NO INHERIT",
            "constraint \"t_c_fkey\" of relation \"t\" is not a not-null constraint",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT t_b_check INHERIT",
            "constraint \"t_b_check\" of relation \"t\" is not a not-null constraint",
        ),
    ];
    for (sql, message) in cases {
        assert!(err_message(&mut s, sql).await == message, "{sql}");
        assert!(err_code(&mut s, sql).await == "42809", "{sql}");
    }
}

// A not-null flag is copied to every child, so `INHERIT` asks for what Crabka
// already does and is a no-op; `NO INHERIT` asks for what it cannot express and
// is refused.
#[tokio::test]
async fn alter_constraint_inheritability_on_a_not_null() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int NOT NULL)"]).await;
    run(
        &mut s,
        "ALTER TABLE t ALTER CONSTRAINT t_a_not_null INHERIT",
    )
    .await;
    assert!(not_null_constraints(&mut s, "t").await == vec![text_row(&["t_a_not_null"])]);
    assert!(
        err_code(
            &mut s,
            "ALTER TABLE t ALTER CONSTRAINT t_a_not_null NO INHERIT"
        )
        .await
            == "0A000"
    );
}

// Enforceability has no counterpart at all: Crabka checks every constraint it
// stores, so the foreign key that would otherwise accept the clause refuses it.
#[tokio::test]
async fn alter_constraint_enforceability_is_refused_on_a_foreign_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int PRIMARY KEY)",
        "CREATE TABLE t (c int REFERENCES p (id))",
    ])
    .await;
    for sql in [
        "ALTER TABLE t ALTER CONSTRAINT t_c_fkey NOT ENFORCED",
        "ALTER TABLE t ALTER CONSTRAINT t_c_fkey ENFORCED",
    ] {
        assert!(err_code(&mut s, sql).await == "0A000", "{sql}");
    }
}

// `NOT VALID` is refused by the grammar itself, before any constraint is looked
// up: PostgreSQL has no way to un-validate a constraint that has been checked.
#[tokio::test]
async fn alter_constraint_cannot_ask_for_not_valid() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    assert!(
        err_message(&mut s, "ALTER TABLE t ALTER CONSTRAINT nope NOT VALID").await
            == "constraints cannot be altered to be NOT VALID"
    );
}

// The two attribute conflicts PostgreSQL words itself, rather than reporting a
// token, reach the client verbatim.
#[tokio::test]
async fn conflicting_attributes_are_refused_by_the_grammar() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    assert!(
        err_message(
            &mut s,
            "ALTER TABLE t ALTER CONSTRAINT c ENFORCED NOT ENFORCED"
        )
        .await
            == "conflicting constraint properties"
    );
    assert!(
        err_message(
            &mut s,
            "ALTER TABLE t ALTER CONSTRAINT c NOT DEFERRABLE INITIALLY DEFERRED"
        )
        .await
            == "constraint declared INITIALLY DEFERRED must be DEFERRABLE"
    );
}

// Changing a foreign key's deferrability is the one property `ALTER CONSTRAINT`
// can actually write. It reaches `pg_constraint` and it moves the check point:
// the violating write goes through, and COMMIT is what fails.
#[tokio::test]
async fn alter_constraint_defers_a_foreign_key_for_real() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int PRIMARY KEY)",
        "CREATE TABLE t (c int REFERENCES p (id))",
    ])
    .await;
    let deferral = "SELECT condeferrable, condeferred FROM pg_constraint \
                    WHERE conname = 't_c_fkey'";
    assert!(query(&mut s, deferral).await == vec![text_row(&["f", "f"])]);
    assert!(err_code(&mut s, "INSERT INTO t VALUES (7)").await == "23503");

    run(
        &mut s,
        "ALTER TABLE t ALTER CONSTRAINT t_c_fkey DEFERRABLE INITIALLY DEFERRED",
    )
    .await;
    assert!(query(&mut s, deferral).await == vec![text_row(&["t", "t"])]);

    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO t VALUES (7)").await;
    assert!(err_code(&mut s, "COMMIT").await == "23503");

    // And back again: the write is refused where it is written once more.
    run(
        &mut s,
        "ALTER TABLE t ALTER CONSTRAINT t_c_fkey NOT DEFERRABLE",
    )
    .await;
    assert!(query(&mut s, deferral).await == vec![text_row(&["f", "f"])]);
    assert!(err_code(&mut s, "INSERT INTO t VALUES (7)").await == "23503");
}

// `DEFERRABLE` on its own leaves the constraint immediate until `SET
// CONSTRAINTS` moves it, which is how PostgreSQL separates the two clauses.
#[tokio::test]
async fn alter_constraint_deferrable_alone_stays_immediate() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int PRIMARY KEY)",
        "CREATE TABLE t (c int REFERENCES p (id))",
        "ALTER TABLE t ALTER CONSTRAINT t_c_fkey DEFERRABLE",
    ])
    .await;
    assert!(
        query(
            &mut s,
            "SELECT condeferrable, condeferred FROM pg_constraint WHERE conname = 't_c_fkey'"
        )
        .await
            == vec![text_row(&["t", "f"])]
    );
    assert!(err_code(&mut s, "INSERT INTO t VALUES (7)").await == "23503");

    run(&mut s, "BEGIN").await;
    run(&mut s, "SET CONSTRAINTS t_c_fkey DEFERRED").await;
    run(&mut s, "INSERT INTO t VALUES (7)").await;
    assert!(err_code(&mut s, "COMMIT").await == "23503");
}
