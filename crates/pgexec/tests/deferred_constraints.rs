//! Constraint deferral.
//!
//! The tests cover `DEFERRABLE INITIALLY DEFERRED` foreign keys checked at
//! `COMMIT`, `SET CONSTRAINTS` that moves the check point inside a transaction,
//! and the savepoint interaction. Every expectation is the behaviour of a live
//! `PostgreSQL` 18.4.

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

// Run each statement of an open block in order and then COMMIT, stopping at the
// first failure and reporting it as (statement, SQLSTATE). Where the violation
// surfaces is the whole subject here, so the statement that reports it is part
// of the expectation.
async fn run_block(s: &mut SqlSession, block: &[&'static str]) -> Option<(&'static str, String)> {
    for sql in block {
        if let Err(error) = s.simple_query(sql).await {
            return Some((*sql, error.code));
        }
    }
    if let Err(error) = s.simple_query("COMMIT").await {
        return Some(("COMMIT", error.code));
    }
    None
}

fn expected_failure(
    failure: Option<(&'static str, &'static str)>,
) -> Option<(&'static str, String)> {
    failure.map(|(sql, code)| (sql, code.to_string()))
}

// A parent/child pair whose foreign key is spelled `tail`.
async fn pair_with(tail: &str) -> (SqlEngine, SqlSession) {
    engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        &format!("CREATE TABLE c (a int4 REFERENCES p (id) {tail})"),
    ])
    .await
}

// ---------------------------------------------------------------------------
// The deferred check point

/// The main case: two relations that reference each other cannot be populated
/// at all unless the checks wait for `COMMIT`. Each row satisfies the other by
/// then, so the transaction commits.
#[tokio::test]
async fn circular_deferred_references_commit_when_each_satisfies_the_other() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE a (id int4 PRIMARY KEY, b_id int4)",
        "CREATE TABLE b (id int4 PRIMARY KEY, a_id int4)",
        "ALTER TABLE a ADD CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES b (id) \
         DEFERRABLE INITIALLY DEFERRED",
        "ALTER TABLE b ADD CONSTRAINT b_a_fk FOREIGN KEY (a_id) REFERENCES a (id) \
         DEFERRABLE INITIALLY DEFERRED",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO a VALUES (1, 1)").await;
    run(&mut s, "INSERT INTO b VALUES (1, 1)").await;
    run(&mut s, "COMMIT").await;

    assert!(query(&mut s, "SELECT id, b_id FROM a").await == vec![text_row(&["1", "1"])]);
    assert!(query(&mut s, "SELECT id, a_id FROM b").await == vec![text_row(&["1", "1"])]);
}

/// The same pair with only one side supplied. The violation appears at
/// `COMMIT` as an ordinary 23503, and neither row is committed. The block's
/// rows are durable, but no clog entry ever makes them visible.
#[tokio::test]
async fn a_deferred_violation_fails_the_commit_and_commits_no_rows() {
    let (_engine, mut s) = pair_with("DEFERRABLE INITIALLY DEFERRED").await;
    run(&mut s, "INSERT INTO p VALUES (1)").await;

    run(&mut s, "BEGIN").await;
    // Accepted at the statement, because the check now waits for COMMIT.
    run(&mut s, "INSERT INTO c VALUES (7)").await;
    assert!(err_code(&mut s, "COMMIT").await == "23503");

    // The block is over and the row never became visible.
    assert!(query(&mut s, "SELECT a FROM c").await.is_empty());
    // The session is usable again straight away, as after any failed COMMIT.
    run(&mut s, "INSERT INTO c VALUES (1)").await;
    assert!(query(&mut s, "SELECT a FROM c").await == vec![text_row(&["1"])]);
}

/// `INITIALLY DEFERRED` in autocommit is checked at the end of the statement,
/// which is the same instant as the implicit commit. Nothing leaves the
/// statement queue when no block is open.
#[tokio::test]
async fn an_initially_deferred_constraint_still_fires_in_autocommit() {
    let (_engine, mut s) = pair_with("DEFERRABLE INITIALLY DEFERRED").await;
    assert!(err_code(&mut s, "INSERT INTO c VALUES (7)").await == "23503");
    assert!(query(&mut s, "SELECT a FROM c").await.is_empty());
}

/// A `NOT DEFERRABLE` constraint ignores the block completely. The check fires
/// at the end of the statement that queued it.
#[tokio::test]
async fn a_non_deferrable_constraint_fires_at_the_statement_inside_a_block() {
    let (_engine, mut s) = pair_with("NOT DEFERRABLE").await;
    run(&mut s, "BEGIN").await;
    assert!(err_code(&mut s, "INSERT INTO c VALUES (7)").await == "23503");
    run(&mut s, "ROLLBACK").await;
}

// ---------------------------------------------------------------------------
// SET CONSTRAINTS

/// `SET CONSTRAINTS ALL DEFERRED` moves a `DEFERRABLE INITIALLY IMMEDIATE`
/// constraint's check to `COMMIT`. `ALL IMMEDIATE` moves it back and drains
/// what is already pending, and it raises there and not at `COMMIT`.
#[tokio::test]
async fn set_constraints_all_moves_the_check_point() {
    struct Case {
        /// Statements run inside the block, in order.
        block: &'static [&'static str],
        /// The statement expected to fail, and its SQLSTATE. `None` when the
        /// block commits.
        expect: Option<(&'static str, &'static str)>,
        why: &'static str,
    }
    let cases = [
        Case {
            block: &["INSERT INTO c VALUES (7)"],
            expect: Some(("INSERT INTO c VALUES (7)", "23503")),
            why: "DEFERRABLE INITIALLY IMMEDIATE checks at the statement",
        },
        Case {
            block: &["SET CONSTRAINTS ALL DEFERRED", "INSERT INTO c VALUES (7)"],
            expect: Some(("COMMIT", "23503")),
            why: "ALL DEFERRED pushes the check out to COMMIT",
        },
        Case {
            block: &[
                "SET CONSTRAINTS ALL DEFERRED",
                "INSERT INTO c VALUES (7)",
                "SET CONSTRAINTS ALL IMMEDIATE",
            ],
            expect: Some(("SET CONSTRAINTS ALL IMMEDIATE", "23503")),
            why: "ALL IMMEDIATE drains what is pending, mid-transaction",
        },
        Case {
            block: &[
                "SET CONSTRAINTS ALL DEFERRED",
                "INSERT INTO c VALUES (7)",
                "INSERT INTO p VALUES (7)",
                "SET CONSTRAINTS ALL IMMEDIATE",
            ],
            expect: None,
            why: "the parent supplied before the drain satisfies the check",
        },
    ];

    for case in cases {
        let (_engine, mut s) = pair_with("DEFERRABLE INITIALLY IMMEDIATE").await;
        run(&mut s, "BEGIN").await;
        let observed = run_block(&mut s, case.block).await;
        assert!(observed == expected_failure(case.expect), "{}", case.why);
        let _ = s.simple_query("ROLLBACK").await;
    }
}

/// `SET CONSTRAINTS <name> IMMEDIATE` drains that constraint's pending entries
/// and leaves every other constraint deferred.
#[tokio::test]
async fn set_constraints_named_immediate_drains_only_its_own_constraint() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c1 (a int4 CONSTRAINT c1_fk REFERENCES p (id) \
         DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE c2 (a int4 CONSTRAINT c2_fk REFERENCES p (id) \
         DEFERRABLE INITIALLY DEFERRED)",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO c1 VALUES (7)").await;
    run(&mut s, "INSERT INTO c2 VALUES (8)").await;
    // c2's entry is untouched, so this drains exactly one violation.
    assert!(err_code(&mut s, "SET CONSTRAINTS c1_fk IMMEDIATE").await == "23503");
    run(&mut s, "ROLLBACK").await;

    // The mirror image: with c1 satisfied, the named drain passes and c2's
    // violation still waits for COMMIT.
    run(&mut s, "INSERT INTO p VALUES (7)").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO c1 VALUES (7)").await;
    run(&mut s, "INSERT INTO c2 VALUES (8)").await;
    run(&mut s, "SET CONSTRAINTS c1_fk IMMEDIATE").await;
    assert!(err_code(&mut s, "COMMIT").await == "23503");
    assert!(query(&mut s, "SELECT a FROM c1").await.is_empty());
}

/// The two name-resolution errors, verbatim from the oracle.
#[tokio::test]
async fn set_constraints_reports_unknown_and_non_deferrable_names() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (a int4 CONSTRAINT c_a_fkey REFERENCES p (id))",
        "CREATE TABLE d (a int4 CONSTRAINT d_a_fkey REFERENCES p (id) \
         DEFERRABLE INITIALLY DEFERRED)",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    assert!(err_code(&mut s, "SET CONSTRAINTS fk_nosuch DEFERRED").await == "42704");
    run(&mut s, "ROLLBACK").await;
    run(&mut s, "BEGIN").await;
    assert!(
        err_message(&mut s, "SET CONSTRAINTS fk_nosuch DEFERRED").await
            == "constraint \"fk_nosuch\" does not exist"
    );
    run(&mut s, "ROLLBACK").await;

    run(&mut s, "BEGIN").await;
    assert!(err_code(&mut s, "SET CONSTRAINTS c_a_fkey DEFERRED").await == "42809");
    run(&mut s, "ROLLBACK").await;
    run(&mut s, "BEGIN").await;
    assert!(
        err_message(&mut s, "SET CONSTRAINTS c_a_fkey DEFERRED").await
            == "constraint \"c_a_fkey\" is not deferrable"
    );
    run(&mut s, "ROLLBACK").await;

    // A deferrable one resolves, inside a block and out of it: PostgreSQL warns
    // outside a block but still validates the name, and this engine has no
    // warning path to emit that on.
    run(&mut s, "SET CONSTRAINTS d_a_fkey DEFERRED").await;
    assert!(err_code(&mut s, "SET CONSTRAINTS fk_nosuch DEFERRED").await == "42704");
}

// ---------------------------------------------------------------------------
// Referential actions, which the deferral clause does not reach
//
// `PostgreSQL` creates a constraint's *check* triggers with its declared
// deferrability and its referential-*action* triggers non-deferrable, whatever
// the clause says. `pg_trigger` on an 18.4 with one deferred constraint per
// action shows `RI_FKey_check_ins`/`_upd` and `RI_FKey_noaction_del`/`_upd` with
// `tgdeferrable = t`, and `RI_FKey_restrict_del`, `_cascade_del`, `_setnull_del`
// and `_setdefault_del` with `tgdeferrable = f`. So a deferred `ON DELETE
// CASCADE` has already deleted its children by the next statement of the block.

/// A `DEFERRABLE INITIALLY DEFERRED ON DELETE CASCADE` deletes its children
/// inside the `DELETE` statement, and the rest of the block sees them gone.
#[tokio::test]
async fn a_deferred_cascade_runs_at_the_statement_not_at_commit() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) ON DELETE CASCADE \
         DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO p VALUES (1), (2)",
        "INSERT INTO c VALUES (10, 1), (11, 1), (12, 2)",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "DELETE FROM p WHERE id = 1").await;
    // Mid-transaction, with nothing having drained the deferred queue.
    assert!(query(&mut s, "SELECT id FROM c ORDER BY id").await == vec![text_row(&["12"])]);
    run(&mut s, "COMMIT").await;

    assert!(query(&mut s, "SELECT id FROM c ORDER BY id").await == vec![text_row(&["12"])]);
    assert!(query(&mut s, "SELECT id FROM p").await == vec![text_row(&["2"])]);
}

/// The other two writing actions answer the same way: the row is nulled or
/// re-defaulted at the `DELETE`, not at `COMMIT`.
#[tokio::test]
async fn a_deferred_set_null_and_set_default_also_run_at_the_statement() {
    for (action, expected) in [("SET NULL", None), ("SET DEFAULT", Some("9"))] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (id int4 PRIMARY KEY, a int4 DEFAULT 9 REFERENCES p (id) \
                 ON DELETE {action} DEFERRABLE INITIALLY DEFERRED)"
            ),
            "INSERT INTO p VALUES (1), (9)",
            "INSERT INTO c VALUES (100, 1)",
        ])
        .await;

        run(&mut s, "BEGIN").await;
        run(&mut s, "DELETE FROM p WHERE id = 1").await;
        let expected = vec![vec![expected.map(ToString::to_string)]];
        assert!(
            query(&mut s, "SELECT a FROM c").await == expected,
            "{action}"
        );
        run(&mut s, "COMMIT").await;
        assert!(
            query(&mut s, "SELECT a FROM c").await == expected,
            "{action}"
        );
    }
}

/// A three-level chain of deferred `ON DELETE CASCADE` runs end to end inside
/// the one `DELETE`. It is not a level per drain, and it is not at `COMMIT`.
#[tokio::test]
async fn a_deferred_cascade_chain_reaches_the_leaf_inside_the_statement() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE anc (id int4 PRIMARY KEY)",
        "CREATE TABLE mid (id int4 PRIMARY KEY, a int4 REFERENCES anc (id) ON DELETE CASCADE \
         DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE leaf (id int4 PRIMARY KEY, m int4 REFERENCES mid (id) ON DELETE CASCADE \
         DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO anc VALUES (1), (2)",
        "INSERT INTO mid VALUES (10, 1), (20, 2)",
        "INSERT INTO leaf VALUES (100, 10), (200, 20)",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "DELETE FROM anc WHERE id = 1").await;
    assert!(query(&mut s, "SELECT id FROM mid ORDER BY id").await == vec![text_row(&["20"])]);
    assert!(query(&mut s, "SELECT id FROM leaf ORDER BY id").await == vec![text_row(&["200"])]);
    run(&mut s, "COMMIT").await;
    assert!(query(&mut s, "SELECT id FROM mid ORDER BY id").await == vec![text_row(&["20"])]);
    assert!(query(&mut s, "SELECT id FROM leaf ORDER BY id").await == vec![text_row(&["200"])]);
}

/// The split is per trigger, not per constraint. The *check* on a constraint
/// that carries `ON DELETE CASCADE` still waits for `COMMIT`, so an insert with
/// no parent is accepted at the statement and fails the commit.
#[tokio::test]
async fn the_check_on_a_cascading_constraint_still_defers() {
    let (_engine, mut s) = pair_with("ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO c VALUES (7)").await;
    assert!(err_code(&mut s, "COMMIT").await == "23503");
    assert!(query(&mut s, "SELECT a FROM c").await.is_empty());
}

/// When `SET CONSTRAINTS ALL IMMEDIATE` runs, there is no cascade left to
/// drain, because the `DELETE` already did it. The drain is a no-op over the
/// rows the `DELETE` left behind.
#[tokio::test]
async fn set_constraints_immediate_finds_a_deferred_cascade_already_run() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) ON DELETE CASCADE \
         DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO p VALUES (1), (2)",
        "INSERT INTO c VALUES (10, 1), (12, 2)",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "DELETE FROM p WHERE id = 1").await;
    assert!(query(&mut s, "SELECT id FROM c ORDER BY id").await == vec![text_row(&["12"])]);
    run(&mut s, "SET CONSTRAINTS ALL IMMEDIATE").await;
    assert!(query(&mut s, "SELECT id FROM c ORDER BY id").await == vec![text_row(&["12"])]);
    run(&mut s, "COMMIT").await;
    assert!(query(&mut s, "SELECT id FROM c ORDER BY id").await == vec![text_row(&["12"])]);
}

// ---------------------------------------------------------------------------
// The mode split: NO ACTION defers, RESTRICT never does

/// The pair that proves `RESTRICT` is not a synonym for `NO ACTION`. Both are
/// `DEFERRABLE INITIALLY DEFERRED` and both delete a referenced parent and
/// re-supply the key before `COMMIT`. `NO ACTION` defers, re-probes, and finds
/// the re-supplied parent. `RESTRICT` ignores the deferral and fires at the end
/// of the `DELETE` statement, where the key genuinely is still referenced.
#[tokio::test]
async fn deferred_no_action_accepts_a_resupplied_key_and_restrict_does_not() {
    struct Case {
        action: &'static str,
        /// `None` when the sequence commits. If it does not commit, this is the
        /// SQLSTATE and the statement that reports it.
        expect: Option<(&'static str, &'static str)>,
        why: &'static str,
    }
    let cases = [
        Case {
            action: "ON DELETE NO ACTION",
            expect: None,
            why: "NO ACTION defers to COMMIT, which finds the re-supplied parent",
        },
        Case {
            action: "ON DELETE RESTRICT",
            expect: Some(("DELETE FROM p WHERE id = 1", "23001")),
            why: "RESTRICT triggers are never deferrable, so it fires at the DELETE",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (a int4 REFERENCES p (id) {} DEFERRABLE INITIALLY DEFERRED)",
                case.action
            ),
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (1)",
        ])
        .await;

        run(&mut s, "BEGIN").await;
        let observed = run_block(
            &mut s,
            &["DELETE FROM p WHERE id = 1", "INSERT INTO p VALUES (1)"],
        )
        .await;
        assert!(observed == expected_failure(case.expect), "{}", case.why);
        let _ = s.simple_query("ROLLBACK").await;
    }
}

/// Immediate `NO ACTION` and immediate `RESTRICT` behave identically: both fail
/// at the `DELETE`, because an immediate check fires at end of statement, when
/// the key is still referenced and not yet re-supplied. Only the SQLSTATE
/// differs.
#[tokio::test]
async fn an_immediate_no_action_and_restrict_both_fire_at_the_delete() {
    for (action, code) in [("NO ACTION", "23503"), ("RESTRICT", "23001")] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!("CREATE TABLE c (a int4 REFERENCES p (id) ON DELETE {action})"),
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (1)",
        ])
        .await;
        run(&mut s, "BEGIN").await;
        assert!(err_code(&mut s, "DELETE FROM p WHERE id = 1").await == code);
        run(&mut s, "ROLLBACK").await;
    }
}

// ---------------------------------------------------------------------------
// Savepoints

/// `SET CONSTRAINTS` is a utility statement, so `ROLLBACK TO SAVEPOINT` undoes
/// it. A check that a rolled-back sub-transaction deferred fires at the
/// statement again.
#[tokio::test]
async fn rollback_to_savepoint_restores_the_deferral_modes() {
    let (_engine, mut s) = pair_with("DEFERRABLE INITIALLY IMMEDIATE").await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "SAVEPOINT sp").await;
    run(&mut s, "SET CONSTRAINTS ALL DEFERRED").await;
    run(&mut s, "ROLLBACK TO SAVEPOINT sp").await;
    // Back to INITIALLY IMMEDIATE, so the check fires at the statement again.
    assert!(err_code(&mut s, "INSERT INTO c VALUES (7)").await == "23503");
    run(&mut s, "ROLLBACK").await;

    // And the other direction: a mode set before the savepoint survives it.
    run(&mut s, "BEGIN").await;
    run(&mut s, "SET CONSTRAINTS ALL DEFERRED").await;
    run(&mut s, "SAVEPOINT sp").await;
    run(&mut s, "ROLLBACK TO SAVEPOINT sp").await;
    run(&mut s, "INSERT INTO c VALUES (7)").await;
    assert!(err_code(&mut s, "COMMIT").await == "23503");
}

/// A rollback of a row-modifying sub-transaction removes both its row and the
/// deferred check that row queued.
#[tokio::test]
async fn rollback_to_savepoint_unwinds_a_queued_check() {
    let (_engine, mut s) = pair_with("DEFERRABLE INITIALLY DEFERRED").await;
    run(&mut s, "INSERT INTO p VALUES (1)").await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "SAVEPOINT sp").await;
    run(&mut s, "INSERT INTO c VALUES (1)").await;
    run(&mut s, "ROLLBACK TO SAVEPOINT sp").await;
    run(&mut s, "DELETE FROM p WHERE id = 1").await;
    run(&mut s, "COMMIT").await;
    assert!(query(&mut s, "SELECT a FROM c").await.is_empty());
}

// ---------------------------------------------------------------------------
// Extended protocol

/// `ALTER TABLE … ADD FOREIGN KEY` and `SET CONSTRAINTS` describe as zero-field
/// results. They do not raise an error at `Parse`.
#[tokio::test]
async fn foreign_key_utility_statements_describe_as_zero_field_results() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (a int4)",
    ])
    .await;

    for (name, sql) in [
        (
            "add_fk",
            "ALTER TABLE c ADD FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE INITIALLY DEFERRED",
        ),
        ("set_all", "SET CONSTRAINTS ALL DEFERRED"),
        ("set_named", "SET CONSTRAINTS c_a_fkey IMMEDIATE"),
    ] {
        let described = s.parse(name, sql, &[]).await.expect("parse should succeed");
        assert!(described.fields.is_empty(), "{sql} describes no fields");
        assert!(
            described.parameter_types.is_empty(),
            "{sql} takes no parameters"
        );
    }
}

// ---------------------------------------------------------------------------
// Deferrable PRIMARY KEY and UNIQUE

/// Five rows keyed 0..4 behind a `UNIQUE … DEFERRABLE` constraint, the shape
/// upstream's `constraints` regression test uses.
async fn ladder(clause: &str) -> (SqlEngine, SqlSession) {
    let create = format!("CREATE TABLE unique_tbl (i int UNIQUE {clause}, t text)");
    engine_with(&[
        &create,
        "INSERT INTO unique_tbl VALUES (0, 'one')",
        "INSERT INTO unique_tbl VALUES (1, 'two')",
        "INSERT INTO unique_tbl VALUES (2, 'tree')",
        "INSERT INTO unique_tbl VALUES (3, 'four')",
        "INSERT INTO unique_tbl VALUES (4, 'five')",
    ])
    .await
}

/// The defect this family exists for: a `DEFERRABLE` key is checked once the
/// statement's rows are all in place, so a shift that never lands two rows on
/// one key succeeds even though every intermediate row does collide.
///
/// The same statement against a `NOT DEFERRABLE` key is the 23505 it has always
/// been, which is what makes this a property of the constraint and not of the
/// statement.
#[tokio::test]
async fn a_deferrable_key_is_checked_once_the_statement_is_done() {
    let (_engine, mut s) = ladder("DEFERRABLE").await;
    run(&mut s, "UPDATE unique_tbl SET i = i+1").await;
    assert!(
        query(&mut s, "SELECT i FROM unique_tbl ORDER BY i").await
            == [
                text_row(&["1"]),
                text_row(&["2"]),
                text_row(&["3"]),
                text_row(&["4"]),
                text_row(&["5"]),
            ]
    );

    let (_engine, mut s) = ladder("NOT DEFERRABLE").await;
    assert!(err_code(&mut s, "UPDATE unique_tbl SET i = i+1").await == "23505");
}

/// `DEFERRABLE` alone is `INITIALLY IMMEDIATE`: a collision the statement
/// leaves standing is reported by that statement, not held to `COMMIT`.
#[tokio::test]
async fn a_deferrable_key_still_reports_within_the_statement() {
    let (_engine, mut s) = ladder("DEFERRABLE").await;
    run(&mut s, "BEGIN").await;
    let failure = err_message(&mut s, "UPDATE unique_tbl SET i = 1 WHERE i = 0").await;
    assert!(failure == "duplicate key value violates unique constraint \"unique_tbl_i_key\"");
    run(&mut s, "ROLLBACK").await;
}

/// `SET CONSTRAINTS … DEFERRED` names a unique constraint by the same name
/// `pg_constraint` reports, and moves its check to `COMMIT`. A transaction that
/// repairs the collision before then commits.
#[tokio::test]
async fn set_constraints_defers_a_unique_key_to_commit() {
    let (_engine, mut s) = ladder("DEFERRABLE").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "SET CONSTRAINTS unique_tbl_i_key DEFERRED").await;
    run(&mut s, "INSERT INTO unique_tbl VALUES (2, 'two again')").await;
    run(&mut s, "DELETE FROM unique_tbl WHERE t = 'tree'").await;
    run(&mut s, "COMMIT").await;
    assert!(
        query(&mut s, "SELECT t FROM unique_tbl WHERE i = 2").await == [text_row(&["two again"])]
    );
}

/// A collision left standing at `COMMIT` fails the `COMMIT`, and the whole
/// transaction is discarded with it.
#[tokio::test]
async fn an_unrepaired_deferred_key_fails_the_commit() {
    let (_engine, mut s) = ladder("DEFERRABLE INITIALLY DEFERRED").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO unique_tbl VALUES (3, 'Three')").await;
    assert!(err_code(&mut s, "COMMIT").await == "23505");
    assert!(query(&mut s, "SELECT t FROM unique_tbl WHERE i = 3").await == [text_row(&["four"])]);
}

/// `SET CONSTRAINTS ALL IMMEDIATE` is a check point of its own: it reports the
/// violation there, inside the transaction, rather than at `COMMIT`.
#[tokio::test]
async fn set_constraints_immediate_drains_a_deferred_unique_key() {
    let (_engine, mut s) = ladder("DEFERRABLE INITIALLY DEFERRED").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO unique_tbl VALUES (3, 'Three')").await;
    assert!(err_code(&mut s, "SET CONSTRAINTS ALL IMMEDIATE").await == "23505");
    run(&mut s, "ROLLBACK").await;
}

/// `SET CONSTRAINTS` refuses a unique constraint that was never declared
/// deferrable, exactly as it refuses a foreign key that was not.
#[tokio::test]
async fn set_constraints_refuses_a_key_that_is_not_deferrable() {
    let (_engine, mut s) = ladder("NOT DEFERRABLE").await;
    run(&mut s, "BEGIN").await;
    let failure = err_message(&mut s, "SET CONSTRAINTS unique_tbl_i_key DEFERRED").await;
    assert!(failure == "constraint \"unique_tbl_i_key\" is not deferrable");
    run(&mut s, "ROLLBACK").await;
}

/// The catalog reports the deferrability it was given, through all three of the
/// places a client reads it: `pg_constraint`, `pg_get_constraintdef`, and
/// `information_schema.table_constraints`.
#[tokio::test]
async fn the_catalog_reports_a_deferrable_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE k (a int UNIQUE DEFERRABLE INITIALLY DEFERRED, \
         b int UNIQUE DEFERRABLE, c int UNIQUE)",
    ])
    .await;

    assert!(
        query(
            &mut s,
            "SELECT conname, condeferrable, condeferred, pg_get_constraintdef(oid) \
             FROM pg_constraint WHERE conrelid = 'k'::regclass ORDER BY conname",
        )
        .await
            == [
                text_row(&[
                    "k_a_key",
                    "t",
                    "t",
                    "UNIQUE (a) DEFERRABLE INITIALLY DEFERRED"
                ]),
                text_row(&["k_b_key", "t", "f", "UNIQUE (b) DEFERRABLE"]),
                text_row(&["k_c_key", "f", "f", "UNIQUE (c)"]),
            ]
    );
    assert!(
        query(
            &mut s,
            "SELECT constraint_name, is_deferrable, initially_deferred \
             FROM information_schema.table_constraints \
             WHERE table_name = 'k' ORDER BY constraint_name",
        )
        .await
            == [
                text_row(&["k_a_key", "YES", "YES"]),
                text_row(&["k_b_key", "YES", "NO"]),
                text_row(&["k_c_key", "NO", "NO"]),
            ]
    );
}

/// Speculative insertion has to decide the row's fate before the statement
/// ends, which is earlier than a deferrable key will answer, so `ON CONFLICT`
/// refuses one as an arbiter — named or inferred, and for `DO NOTHING` too,
/// whose arbiter set is every unique index the relation has.
#[tokio::test]
async fn on_conflict_refuses_a_deferrable_key_as_an_arbiter() {
    let (_engine, mut s) = ladder("DEFERRABLE").await;
    for sql in [
        "INSERT INTO unique_tbl VALUES (0, 'x') ON CONFLICT (i) DO NOTHING",
        "INSERT INTO unique_tbl VALUES (0, 'x') ON CONFLICT ON CONSTRAINT unique_tbl_i_key \
         DO UPDATE SET t = 'x'",
        "INSERT INTO unique_tbl VALUES (0, 'x') ON CONFLICT DO NOTHING",
    ] {
        let error = s.simple_query(sql).await.expect_err("expected error");
        assert!(error.code == "55000", "{sql}");
        assert!(
            error.message
                == "ON CONFLICT does not support deferrable unique constraints/exclusion \
                    constraints as arbiters",
            "{sql}"
        );
    }
}

/// `ALTER TABLE … ADD CONSTRAINT` records deferrability too, and still
/// back-validates the rows already stored — the index build is what validates
/// them, and it is not deferrable.
#[tokio::test]
async fn alter_table_adds_a_deferrable_key_and_still_back_validates() {
    let (_engine, mut s) = ladder("NOT DEFERRABLE").await;
    run(
        &mut s,
        "ALTER TABLE unique_tbl DROP CONSTRAINT unique_tbl_i_key",
    )
    .await;
    run(
        &mut s,
        "ALTER TABLE unique_tbl ADD CONSTRAINT unique_tbl_i_key UNIQUE (i) \
         DEFERRABLE INITIALLY DEFERRED",
    )
    .await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO unique_tbl VALUES (1, 'five')").await;
    run(&mut s, "INSERT INTO unique_tbl VALUES (5, 'one')").await;
    run(&mut s, "DELETE FROM unique_tbl WHERE i = 1 AND t = 'two'").await;
    run(&mut s, "COMMIT").await;

    run(&mut s, "INSERT INTO unique_tbl VALUES (9, 'nine')").await;
    run(
        &mut s,
        "ALTER TABLE unique_tbl DROP CONSTRAINT unique_tbl_i_key",
    )
    .await;
    run(&mut s, "INSERT INTO unique_tbl VALUES (9, 'nine again')").await;
    assert!(
        err_code(
            &mut s,
            "ALTER TABLE unique_tbl ADD CONSTRAINT unique_tbl_i_key UNIQUE (i) \
             DEFERRABLE INITIALLY DEFERRED",
        )
        .await
            == "23505"
    );
}

/// Deferring the check to the end of the statement must not lose it. Two rows
/// of one `INSERT` landing on one key still collide, and so does a `COPY`,
/// whose whole load is one command.
#[tokio::test]
async fn a_deferrable_key_still_catches_a_collision_inside_one_command() {
    let (_engine, mut s) = ladder("DEFERRABLE").await;
    assert!(err_code(&mut s, "INSERT INTO unique_tbl VALUES (7, 'a'), (7, 'b')").await == "23505");
    assert!(
        query(&mut s, "SELECT i FROM unique_tbl WHERE i = 7")
            .await
            .is_empty()
    );

    // And through a feeding query rather than a VALUES list.
    assert!(
        err_code(
            &mut s,
            "INSERT INTO unique_tbl SELECT 8, t FROM unique_tbl WHERE i < 2",
        )
        .await
            == "23505"
    );
}

/// A `DELETE` in the same statement frees the key an `INSERT` in that statement
/// takes, which is the end-of-statement check point doing its job rather than
/// an ordering accident.
#[tokio::test]
async fn a_deferrable_key_freed_by_the_same_command_is_available_to_it() {
    let (_engine, mut s) = ladder("DEFERRABLE").await;
    run(
        &mut s,
        "WITH gone AS (DELETE FROM unique_tbl WHERE i = 2 RETURNING i) \
         INSERT INTO unique_tbl SELECT i, 'moved' FROM gone",
    )
    .await;
    assert!(query(&mut s, "SELECT t FROM unique_tbl WHERE i = 2").await == [text_row(&["moved"])]);
}

/// A `DEFERRABLE` key is not unique for the whole of a referencing statement,
/// so `PostgreSQL` refuses to point a foreign key at one — per the SQL spec,
/// and to avoid the semantics of a parent that momentarily has two rows.
#[tokio::test]
async fn a_foreign_key_cannot_reference_a_deferrable_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int PRIMARY KEY DEFERRABLE, u int UNIQUE DEFERRABLE, v int UNIQUE)",
        "CREATE TABLE c (a int)",
    ])
    .await;

    for (sql, message) in [
        (
            "ALTER TABLE c ADD FOREIGN KEY (a) REFERENCES p",
            "cannot use a deferrable primary key for referenced table \"p\"",
        ),
        (
            "ALTER TABLE c ADD FOREIGN KEY (a) REFERENCES p (u)",
            "cannot use a deferrable unique constraint for referenced table \"p\"",
        ),
    ] {
        let error = s.simple_query(sql).await.expect_err("expected error");
        assert!(error.code == "55000", "{sql}");
        assert!(error.message == message, "{sql}");
    }
    // The immediate key on the same relation still works.
    run(&mut s, "ALTER TABLE c ADD FOREIGN KEY (a) REFERENCES p (v)").await;
}
