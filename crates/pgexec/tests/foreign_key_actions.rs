//! Referential actions: the five `ON DELETE` / `ON UPDATE` actions, the
//! `NO ACTION` versus `RESTRICT` split that only deferral makes visible, and the
//! termination of a cascade that walks back into itself. Every expectation here
//! was captured from a live `PostgreSQL` 18.4 — SQLSTATEs, primary messages and
//! `DETAIL` lines are that server's, verbatim.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, QueryResult, Session},
    error::PgError,
};

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

async fn error(s: &mut SqlSession, sql: &str) -> PgError {
    s.simple_query(sql).await.expect_err("expected error")
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn row(values: &[Option<&str>]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|v| v.map(std::string::ToString::to_string))
        .collect()
}

fn rows(values: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
    values.iter().map(|values| row(values)).collect()
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

fn no_rows() -> Vec<Vec<Option<String>>> {
    Vec::new()
}

/// The parent-side 23503 `NO ACTION` reports for key 1 of `p`, still referenced
/// from `c`.
///
/// The `DETAIL` names the *referenced* columns — `p`'s `id`, not `c`'s `a` —
/// because the key it reports is the parent row's, which is also why one
/// parent-side message can serve every child that references it.
fn no_action_violation() -> PgError {
    PgError::error(
        "23503",
        "update or delete on table \"p\" violates foreign key constraint \
         \"c_a_fkey\" on table \"c\"",
    )
    .with_detail("Key (id)=(1) is still referenced from table \"c\".")
}

/// What `RESTRICT` reports for the same row: a different SQLSTATE (23001,
/// `restrict_violation`), "violates RESTRICT setting of" in the message, and
/// "is referenced" where `NO ACTION` says "is still referenced".
fn restrict_violation() -> PgError {
    PgError::error(
        "23001",
        "update or delete on table \"p\" violates RESTRICT setting of foreign key \
         constraint \"c_a_fkey\" on table \"c\"",
    )
    .with_detail("Key (id)=(1) is referenced from table \"c\".")
}

// ---------------------------------------------------------------------------
// The five actions, on each side

/// A parent/child pair carrying one referential action. `p` holds the key under
/// test (1) and a second key (9) that is also the child's `DEFAULT`, so
/// `SET DEFAULT` has somewhere to land.
async fn pair_with(action: &str) -> (SqlEngine, SqlSession) {
    engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        &format!(
            "CREATE TABLE c (id int4 PRIMARY KEY, a int4 DEFAULT 9 REFERENCES p (id) {action})"
        ),
        "INSERT INTO p VALUES (1), (9)",
        "INSERT INTO c VALUES (100, 1)",
    ])
    .await
}

/// Deleting a referenced parent row runs the child's `ON DELETE` action. All
/// five actions, plus the omitted clause that means `NO ACTION`.
#[tokio::test]
async fn every_on_delete_action_does_its_own_thing() {
    struct Case {
        action: &'static str,
        /// The child after the delete, or the error that refused it.
        expect: Result<Vec<Vec<Option<String>>>, PgError>,
        why: &'static str,
    }
    let cases = [
        Case {
            action: "",
            expect: Err(no_action_violation()),
            why: "an omitted ON DELETE clause is NO ACTION",
        },
        Case {
            action: "ON DELETE NO ACTION",
            expect: Err(no_action_violation()),
            why: "NO ACTION refuses a delete that would orphan a child row",
        },
        Case {
            action: "ON DELETE RESTRICT",
            expect: Err(restrict_violation()),
            why: "RESTRICT refuses it too, but as 23001 and with its own wording",
        },
        Case {
            action: "ON DELETE CASCADE",
            expect: Ok(no_rows()),
            why: "CASCADE deletes the referencing row",
        },
        Case {
            action: "ON DELETE SET NULL",
            expect: Ok(rows(&[&[Some("100"), None]])),
            why: "SET NULL keeps the row and nulls the whole key",
        },
        Case {
            action: "ON DELETE SET DEFAULT",
            expect: Ok(rows(&[&[Some("100"), Some("9")]])),
            why: "SET DEFAULT re-points the row at the column's DEFAULT",
        },
    ];
    for case in cases {
        let (_engine, mut s) = pair_with(case.action).await;
        match case.expect {
            Ok(expected) => {
                run(&mut s, "DELETE FROM p WHERE id = 1").await;
                assert!(
                    query(&mut s, "SELECT id, a FROM c ORDER BY id").await == expected,
                    "{}",
                    case.why
                );
            }
            Err(expected) => {
                assert!(
                    error(&mut s, "DELETE FROM p WHERE id = 1").await == expected,
                    "{}",
                    case.why
                );
                // A refused delete leaves both relations exactly as they were.
                assert!(
                    query(&mut s, "SELECT id, a FROM c ORDER BY id").await
                        == vec![text_row(&["100", "1"])]
                );
                assert!(
                    query(&mut s, "SELECT id FROM p ORDER BY id").await
                        == vec![text_row(&["1"]), text_row(&["9"])]
                );
            }
        }
    }
}

/// Moving a referenced key runs the child's `ON UPDATE` action. The parent-side
/// messages are the same ones a delete reports — `PostgreSQL` words both as
/// "update or delete on table".
#[tokio::test]
async fn every_on_update_action_does_its_own_thing() {
    struct Case {
        action: &'static str,
        expect: Result<Vec<Vec<Option<String>>>, PgError>,
        why: &'static str,
    }
    let cases = [
        Case {
            action: "",
            expect: Err(no_action_violation()),
            why: "an omitted ON UPDATE clause is NO ACTION",
        },
        Case {
            action: "ON UPDATE NO ACTION",
            expect: Err(no_action_violation()),
            why: "NO ACTION refuses a key move that would orphan a child row",
        },
        Case {
            action: "ON UPDATE RESTRICT",
            expect: Err(restrict_violation()),
            why: "RESTRICT refuses it as 23001",
        },
        Case {
            action: "ON UPDATE CASCADE",
            expect: Ok(rows(&[&[Some("100"), Some("5")]])),
            why: "CASCADE carries the new key into the referencing row",
        },
        Case {
            action: "ON UPDATE SET NULL",
            expect: Ok(rows(&[&[Some("100"), None]])),
            why: "SET NULL drops the reference rather than following it",
        },
        Case {
            action: "ON UPDATE SET DEFAULT",
            expect: Ok(rows(&[&[Some("100"), Some("9")]])),
            why: "SET DEFAULT re-points the row at the column's DEFAULT",
        },
    ];
    for case in cases {
        let (_engine, mut s) = pair_with(case.action).await;
        match case.expect {
            Ok(expected) => {
                run(&mut s, "UPDATE p SET id = 5 WHERE id = 1").await;
                assert!(
                    query(&mut s, "SELECT id, a FROM c ORDER BY id").await == expected,
                    "{}",
                    case.why
                );
                assert!(
                    query(&mut s, "SELECT id FROM p ORDER BY id").await
                        == vec![text_row(&["5"]), text_row(&["9"])]
                );
            }
            Err(expected) => {
                assert!(
                    error(&mut s, "UPDATE p SET id = 5 WHERE id = 1").await == expected,
                    "{}",
                    case.why
                );
                assert!(
                    query(&mut s, "SELECT id, a FROM c ORDER BY id").await
                        == vec![text_row(&["100", "1"])]
                );
                assert!(
                    query(&mut s, "SELECT id FROM p ORDER BY id").await
                        == vec![text_row(&["1"]), text_row(&["9"])]
                );
            }
        }
    }
}

/// `ON UPDATE CASCADE` follows the key through every referencing row, and a
/// non-key update of the parent moves nothing.
#[tokio::test]
async fn on_update_cascade_propagates_a_key_change_to_every_child() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY, note int4)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) ON UPDATE CASCADE)",
        "INSERT INTO p VALUES (1, 10), (2, 20)",
        "INSERT INTO c VALUES (100, 1), (101, 1), (102, 2)",
    ])
    .await;
    // A non-key update of the parent is invisible to the constraint.
    run(&mut s, "UPDATE p SET note = 11 WHERE id = 1").await;
    assert!(
        query(&mut s, "SELECT id, a FROM c ORDER BY id").await
            == rows(&[
                &[Some("100"), Some("1")],
                &[Some("101"), Some("1")],
                &[Some("102"), Some("2")],
            ])
    );
    run(&mut s, "UPDATE p SET id = 7 WHERE id = 1").await;
    assert!(
        query(&mut s, "SELECT id, a FROM c ORDER BY id").await
            == rows(&[
                &[Some("100"), Some("7")],
                &[Some("101"), Some("7")],
                &[Some("102"), Some("2")],
            ])
    );
}

// ---------------------------------------------------------------------------
// NO ACTION versus RESTRICT

/// The pair for the deferral matrix: one child row referencing the one parent
/// row, under whatever constraint tail the case names.
async fn deferral_pair(tail: &str) -> (SqlEngine, SqlSession) {
    engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        &format!("CREATE TABLE c (a int4 REFERENCES p (id) {tail})"),
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (1)",
    ])
    .await
}

/// Run an already-`BEGIN`-ning block in order and then `COMMIT`, reporting the
/// first statement that failed and its error. *Where* the failure lands is the
/// whole subject of the deferral cases, so the statement is part of the answer.
/// The transaction is always closed, leaving the session usable.
async fn run_block(s: &mut SqlSession, block: &[&'static str]) -> Option<(&'static str, PgError)> {
    for sql in block {
        if let Err(e) = s.simple_query(sql).await {
            let _ = s.simple_query("ROLLBACK").await;
            return Some((sql, e));
        }
    }
    match s.simple_query("COMMIT").await {
        // A COMMIT that reports a deferred violation has already ended the
        // transaction; there is nothing left to roll back.
        Err(e) => Some(("COMMIT", e)),
        Ok(_) => None,
    }
}

/// `NO ACTION` and `RESTRICT` are not synonyms, and while the constraint is
/// immediate the only difference is the error they raise. Deferral is what
/// separates them behaviourally: `NO ACTION` inherits the constraint's
/// deferrability and waits for `COMMIT`, while `RESTRICT`'s trigger is created
/// non-deferrable whatever the clause says, so it fires at end of statement
/// anyway.
#[tokio::test]
async fn no_action_and_restrict_diverge_only_under_deferral() {
    struct Case {
        tail: &'static str,
        /// The statement that failed, and the error it raised.
        expect: (&'static str, PgError),
        why: &'static str,
    }
    let cases = [
        Case {
            tail: "ON DELETE NO ACTION",
            expect: ("DELETE FROM p WHERE id = 1", no_action_violation()),
            why: "an immediate NO ACTION fires at end of statement",
        },
        Case {
            tail: "ON DELETE RESTRICT",
            expect: ("DELETE FROM p WHERE id = 1", restrict_violation()),
            why: "an immediate RESTRICT fires there too, as 23001",
        },
        Case {
            tail: "ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED",
            expect: ("COMMIT", no_action_violation()),
            why: "NO ACTION defers, and the violation surfaces at COMMIT",
        },
        Case {
            tail: "ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED",
            expect: ("DELETE FROM p WHERE id = 1", restrict_violation()),
            why: "RESTRICT ignores the deferral and still fires at end of statement",
        },
    ];
    for case in cases {
        let (_engine, mut s) = deferral_pair(case.tail).await;
        let failure = run_block(&mut s, &["BEGIN", "DELETE FROM p WHERE id = 1"]).await;
        assert!(failure == Some(case.expect), "{}", case.why);
        // Whichever end of the transaction refused it, nothing was committed.
        assert!(query(&mut s, "SELECT id FROM p").await == vec![text_row(&["1"])]);
        assert!(query(&mut s, "SELECT a FROM c").await == vec![text_row(&["1"])]);
    }
}

/// Deleting a referenced key and putting it back inside one transaction is
/// accepted only when the check is deferred to `COMMIT`, where the re-probe
/// finds the re-supplied row. The idiom is a property of *deferral*, not of
/// `NO ACTION`: an immediate `NO ACTION` never sees the second statement, and a
/// deferred `RESTRICT` does not defer in the first place.
#[tokio::test]
async fn a_re_supplied_key_is_accepted_only_by_a_deferred_check() {
    struct Case {
        tail: &'static str,
        expect: Option<(&'static str, PgError)>,
        why: &'static str,
    }
    let cases = [
        Case {
            tail: "ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED",
            expect: None,
            why: "the COMMIT-time re-probe finds the key the second statement supplied",
        },
        Case {
            tail: "ON DELETE NO ACTION",
            expect: Some(("DELETE FROM p WHERE id = 1", no_action_violation())),
            why: "an immediate check never reaches the re-supplying statement",
        },
        Case {
            tail: "ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED",
            expect: Some(("DELETE FROM p WHERE id = 1", restrict_violation())),
            why: "RESTRICT does not defer, so there is no re-probe to reach",
        },
        Case {
            tail: "ON DELETE RESTRICT",
            expect: Some(("DELETE FROM p WHERE id = 1", restrict_violation())),
            why: "and an immediate RESTRICT is no different",
        },
    ];
    for case in cases {
        let (_engine, mut s) = deferral_pair(case.tail).await;
        let failure = run_block(
            &mut s,
            &[
                "BEGIN",
                "DELETE FROM p WHERE id = 1",
                "INSERT INTO p VALUES (1)",
            ],
        )
        .await;
        assert!(failure == case.expect, "{}", case.why);
        // Either way the committed state is the one we started with: the
        // successful case re-supplied the very key it deleted.
        assert!(query(&mut s, "SELECT id FROM p").await == vec![text_row(&["1"])]);
        assert!(query(&mut s, "SELECT a FROM c").await == vec![text_row(&["1"])]);
    }
}

// ---------------------------------------------------------------------------
// SET NULL and SET DEFAULT

/// `ON DELETE SET NULL (…)` nulls only the columns it names, leaving the rest of
/// the key alone. Under `MATCH SIMPLE` a partly-null key passes, so the row
/// survives.
#[tokio::test]
async fn set_null_with_a_column_list_nulls_only_those_columns() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (x int4, y int4, PRIMARY KEY (x, y))",
        "CREATE TABLE c (a int4, b int4, \
         FOREIGN KEY (a, b) REFERENCES p (x, y) ON DELETE SET NULL (a))",
        "INSERT INTO p VALUES (1, 2)",
        "INSERT INTO c VALUES (1, 2)",
    ])
    .await;
    run(&mut s, "DELETE FROM p WHERE x = 1").await;
    assert!(query(&mut s, "SELECT a, b FROM c").await == rows(&[&[None, Some("2")]]));
}

/// A cascaded `SET NULL` is an ordinary assignment and meets the ordinary
/// not-null check: 23502, not a referential SQLSTATE. (`PostgreSQL` adds a
/// `CONTEXT:` line naming the internal `UPDATE ONLY … SET "a" = NULL` statement,
/// and a `DETAIL:` naming the failing row; this engine emits neither, which is
/// why only the code and the primary message are asserted.)
#[tokio::test]
async fn set_null_onto_a_not_null_column_is_23502() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (a int4 NOT NULL REFERENCES p (id) ON DELETE SET NULL)",
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (1)",
    ])
    .await;
    let e = error(&mut s, "DELETE FROM p WHERE id = 1").await;
    assert!(e.code == "23502");
    assert!(
        e.message == "null value in column \"a\" of relation \"c\" violates not-null constraint"
    );
    assert!(query(&mut s, "SELECT a FROM c").await == vec![text_row(&["1"])]);
}

/// `SET DEFAULT` writes the column's `DEFAULT` and then the write is checked
/// like any other: a default with no parent row is a child-side 23503 naming the
/// default value itself. That is the proof that a cascaded write re-enters the
/// check path rather than bypassing it.
#[tokio::test]
async fn set_default_re_enters_the_check_path() {
    struct Case {
        default: &'static str,
        expect: Result<Vec<Vec<Option<String>>>, PgError>,
        why: &'static str,
    }
    let cases = [
        Case {
            default: "DEFAULT 9",
            expect: Ok(rows(&[&[Some("9")]])),
            why: "9 is a parent key, so the re-queued check passes",
        },
        Case {
            default: "",
            expect: Ok(rows(&[&[None]])),
            why: "an absent DEFAULT is NULL, which no check can fail",
        },
        Case {
            default: "DEFAULT 7",
            expect: Err(PgError::error(
                "23503",
                "insert or update on table \"c\" violates foreign key constraint \"c_a_fkey\"",
            )
            .with_detail("Key (a)=(7) is not present in table \"p\".")),
            why: "7 is not a parent key, so the cascaded write fails its own check",
        },
    ];
    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (a int4 {} REFERENCES p (id) ON DELETE SET DEFAULT)",
                case.default
            ),
            "INSERT INTO p VALUES (1), (9)",
            "INSERT INTO c VALUES (1)",
        ])
        .await;
        match case.expect {
            Ok(expected) => {
                run(&mut s, "DELETE FROM p WHERE id = 1").await;
                assert!(
                    query(&mut s, "SELECT a FROM c").await == expected,
                    "{}",
                    case.why
                );
            }
            Err(expected) => {
                assert!(
                    error(&mut s, "DELETE FROM p WHERE id = 1").await == expected,
                    "{}",
                    case.why
                );
                assert!(query(&mut s, "SELECT a FROM c").await == vec![text_row(&["1"])]);
            }
        }
    }
}

/// A child row another part of the *same command* has already updated still
/// references its parent, so the parent-side check counts it. `PostgreSQL`
/// re-reads the row when the trigger queue runs and reports the ordinary 23503;
/// what the row was touched by is irrelevant, only what its key now holds.
#[tokio::test]
async fn a_child_row_the_command_updated_off_key_still_blocks_the_delete() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id), note int4)",
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (100, 1, 0)",
    ])
    .await;
    let refused = error(
        &mut s,
        "WITH u AS (UPDATE c SET note = 1 WHERE id = 100 RETURNING id) \
         DELETE FROM p WHERE id = 1",
    )
    .await;
    assert!(refused == no_action_violation());
    // Neither half of the command survives the refusal, so no orphan is left.
    assert!(query(&mut s, "SELECT id FROM p").await == vec![text_row(&["1"])]);
    assert!(query(&mut s, "SELECT id, a, note FROM c").await == vec![text_row(&["100", "1", "0"])]);
}

/// The acting half of the same shape. `PostgreSQL` runs a referential action as
/// a command of its own, so a child row an earlier part of the outer command
/// modified off-key is re-read and the action applied to it: the cascade deletes
/// it, and `SET NULL` / `SET DEFAULT` rewrite its key while the command's own
/// change to the rest of the row survives.
#[tokio::test]
async fn a_referential_action_reaches_a_child_row_the_command_modified() {
    struct Case {
        action: &'static str,
        /// `c` after the delete, as `(id, a, note)`.
        expect: Vec<Vec<Option<String>>>,
        why: &'static str,
    }
    let cases = [
        Case {
            action: "ON DELETE CASCADE",
            expect: no_rows(),
            why: "the cascade deletes the row the WITH item had just updated",
        },
        Case {
            action: "ON DELETE SET NULL",
            expect: rows(&[&[Some("100"), None, Some("1")]]),
            why: "the key is nulled and the command's own note = 1 survives",
        },
        Case {
            action: "ON DELETE SET DEFAULT",
            expect: rows(&[&[Some("100"), Some("9"), Some("1")]]),
            why: "the key becomes the column DEFAULT and note = 1 survives",
        },
    ];
    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (id int4 PRIMARY KEY, a int4 DEFAULT 9 REFERENCES p (id) {}, \
                 note int4)",
                case.action
            ),
            "INSERT INTO p VALUES (1), (9)",
            "INSERT INTO c VALUES (100, 1, 0)",
        ])
        .await;
        run(
            &mut s,
            "WITH u AS (UPDATE c SET note = 1 WHERE id = 100 RETURNING id) \
             DELETE FROM p WHERE id = 1",
        )
        .await;
        assert!(
            query(&mut s, "SELECT id, a, note FROM c ORDER BY id").await == case.expect,
            "{}",
            case.why
        );
        // The parent key really went; only 9, the SET DEFAULT target, is left.
        assert!(query(&mut s, "SELECT id FROM p ORDER BY id").await == vec![text_row(&["9"])]);
    }
}

/// The same shape one relation deeper: the row the command modified is the
/// *intermediate* of a two-hop cascade, so reaching it is also what lets the
/// cascade reach the leaf below it.
#[tokio::test]
async fn a_cascade_reaches_an_intermediate_row_the_command_modified() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE gp (id int4 PRIMARY KEY)",
        "CREATE TABLE par (id int4 PRIMARY KEY, g int4 REFERENCES gp (id) ON DELETE CASCADE, \
         note int4)",
        "CREATE TABLE ch (id int4 PRIMARY KEY, p int4 REFERENCES par (id) ON DELETE CASCADE)",
        "INSERT INTO gp VALUES (1)",
        "INSERT INTO par VALUES (10, 1, 0)",
        "INSERT INTO ch VALUES (100, 10)",
    ])
    .await;
    run(
        &mut s,
        "WITH u AS (UPDATE par SET note = 1 WHERE id = 10 RETURNING id) \
         DELETE FROM gp WHERE id = 1",
    )
    .await;
    assert!(query(&mut s, "SELECT id FROM par").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM ch").await == no_rows());
}

// ---------------------------------------------------------------------------
// Several actions reaching one child row

/// Two foreign keys whose actions both land on the same child row both run.
/// `PostgreSQL` issues each referential action as a query of its own against the
/// row's current image, so the second one sees what the first wrote and adds to
/// it rather than replacing it — leaving no column still pointing at a parent
/// that is gone.
///
/// The two shapes are the two ways one command can remove both referenced keys:
/// one `DELETE` of a single parent row that both keys reference, and one command
/// whose `WITH` list and body each empty a different parent relation.
#[tokio::test]
async fn two_actions_on_one_child_row_both_run() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) ON DELETE SET NULL, \
         b int4 REFERENCES p (id) ON DELETE SET NULL)",
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (100, 1, 1)",
    ])
    .await;
    run(&mut s, "DELETE FROM p WHERE id = 1").await;
    assert!(
        query(&mut s, "SELECT id, a, b FROM c").await == rows(&[&[Some("100"), None, None]]),
        "one deleted key referenced twice nulls both referencing columns"
    );

    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p1 (id int4 PRIMARY KEY)",
        "CREATE TABLE p2 (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p1 (id) ON DELETE SET NULL, \
         b int4 REFERENCES p2 (id) ON DELETE SET NULL)",
        "INSERT INTO p1 VALUES (1)",
        "INSERT INTO p2 VALUES (2)",
        "INSERT INTO c VALUES (100, 1, 2)",
    ])
    .await;
    run(
        &mut s,
        "WITH d AS (DELETE FROM p1 WHERE id = 1 RETURNING id) DELETE FROM p2 WHERE id = 2",
    )
    .await;
    assert!(
        query(&mut s, "SELECT id, a, b FROM c").await == rows(&[&[Some("100"), None, None]]),
        "two parent relations emptied by one command null both referencing columns"
    );
}

/// The actions need not be the same one. `SET NULL` and `SET DEFAULT` each write
/// their own column, and a third foreign key joins in without disturbing either.
#[tokio::test]
async fn actions_of_different_kinds_compose_on_one_child_row() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) ON DELETE SET NULL, \
         b int4 DEFAULT 9 REFERENCES p (id) ON DELETE SET DEFAULT)",
        "INSERT INTO p VALUES (1), (9)",
        "INSERT INTO c VALUES (100, 1, 1)",
    ])
    .await;
    run(&mut s, "DELETE FROM p WHERE id = 1").await;
    assert!(
        query(&mut s, "SELECT id, a, b FROM c").await == rows(&[&[Some("100"), None, Some("9")]]),
        "SET NULL nulls its column and SET DEFAULT writes the DEFAULT into its own"
    );

    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) ON DELETE SET NULL, \
         b int4 DEFAULT 9 REFERENCES p (id) ON DELETE SET DEFAULT, \
         c int4 REFERENCES p (id) ON DELETE SET NULL)",
        "INSERT INTO p VALUES (1), (9)",
        "INSERT INTO c VALUES (100, 1, 1, 1)",
    ])
    .await;
    run(&mut s, "DELETE FROM p WHERE id = 1").await;
    assert!(
        query(&mut s, "SELECT id, a, b, c FROM c").await
            == rows(&[&[Some("100"), None, Some("9"), None]]),
        "a third foreign key's action lands on the same row too"
    );
}

/// `CASCADE` alongside `SET NULL` on *different* columns: the row goes, whichever
/// of the two runs first. The cascade deletes a row the `SET NULL` has already
/// rewritten, and a `SET NULL` whose row the cascade already deleted finds
/// nothing left to write.
#[tokio::test]
async fn a_cascade_beside_a_set_null_deletes_the_row_either_way() {
    for (a_action, b_action) in [
        ("ON DELETE CASCADE", "ON DELETE SET NULL"),
        ("ON DELETE SET NULL", "ON DELETE CASCADE"),
    ] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) {a_action}, \
                 b int4 REFERENCES p (id) {b_action})"
            ),
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (100, 1, 1)",
        ])
        .await;
        run(&mut s, "DELETE FROM p WHERE id = 1").await;
        assert!(
            query(&mut s, "SELECT id, a, b FROM c").await == no_rows(),
            "CASCADE wins over SET NULL on another column, in either declaration order"
        );
    }
}

/// Two actions that genuinely conflict, because both foreign keys key on the
/// *same* column: whether the row survives depends on which constraint runs
/// first, and each action's search sees what the earlier one wrote.
///
/// `SET NULL` first leaves the row with no key for the `CASCADE`'s search to
/// match, so the row survives; `CASCADE` first deletes it, and the `SET NULL`
/// then has nothing to update. `PostgreSQL` fires the constraints in creation
/// order and this engine in name order, so the constraints are named to make the
/// two orders agree.
#[tokio::test]
async fn conflicting_actions_on_one_column_resolve_in_constraint_order() {
    struct Case {
        first: &'static str,
        second: &'static str,
        expect: Vec<Vec<Option<String>>>,
        why: &'static str,
    }
    let cases = [
        Case {
            first: "ON DELETE SET NULL",
            second: "ON DELETE CASCADE",
            expect: rows(&[&[Some("100"), None]]),
            why: "the nulled key no longer matches the cascade's search, so the row survives",
        },
        Case {
            first: "ON DELETE CASCADE",
            second: "ON DELETE SET NULL",
            expect: no_rows(),
            why: "the cascade removes the row before the SET NULL can look for it",
        },
    ];
    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (id int4 PRIMARY KEY, a int4, \
                 CONSTRAINT k1 FOREIGN KEY (a) REFERENCES p (id) {}, \
                 CONSTRAINT k2 FOREIGN KEY (a) REFERENCES p (id) {})",
                case.first, case.second
            ),
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (100, 1)",
        ])
        .await;
        run(&mut s, "DELETE FROM p WHERE id = 1").await;
        assert!(
            query(&mut s, "SELECT id, a FROM c").await == case.expect,
            "{}",
            case.why
        );
    }
}

// ---------------------------------------------------------------------------
// Cascades that walk back into themselves

/// Two relations that reference each other with `ON DELETE CASCADE`. The delete
/// of `a`'s row cascades into `b`, whose delete cascades back into the very `a`
/// row the statement already removed — and stops there, because a statement
/// modifies a given row at most once. An unrelated pair is untouched.
#[tokio::test]
async fn a_two_table_cascade_cycle_terminates() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE cyc_a (id int4 PRIMARY KEY, b int4)",
        "CREATE TABLE cyc_b (id int4 PRIMARY KEY, a int4 REFERENCES cyc_a (id) ON DELETE CASCADE)",
        "ALTER TABLE cyc_a ADD CONSTRAINT cyc_a_b_fkey FOREIGN KEY (b) REFERENCES cyc_b (id) \
         ON DELETE CASCADE",
        "INSERT INTO cyc_a VALUES (1, NULL), (2, NULL)",
        "INSERT INTO cyc_b VALUES (10, 1), (20, 2)",
        "UPDATE cyc_a SET b = 10 WHERE id = 1",
        "UPDATE cyc_a SET b = 20 WHERE id = 2",
    ])
    .await;
    run(&mut s, "DELETE FROM cyc_a WHERE id = 1").await;
    assert!(query(&mut s, "SELECT id FROM cyc_a ORDER BY id").await == vec![text_row(&["2"])]);
    assert!(query(&mut s, "SELECT id FROM cyc_b ORDER BY id").await == vec![text_row(&["20"])]);
    // The other half of the cycle drives it just as well.
    run(&mut s, "DELETE FROM cyc_b WHERE id = 20").await;
    assert!(query(&mut s, "SELECT id FROM cyc_a").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM cyc_b").await == no_rows());
}

/// A self-referencing tree, including a row that references itself. The cascade
/// walks down the tree once and converges; the self-reference is the degenerate
/// case where the cascade's first hop is the row it started from.
#[tokio::test]
async fn a_self_referencing_cascade_terminates() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE tree (id int4 PRIMARY KEY, parent int4 REFERENCES tree (id) \
         ON DELETE CASCADE)",
        // A multi-row insert whose later rows reference its earlier ones: the
        // check fires once the statement's rows all exist.
        "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2), (4, 2), (5, NULL)",
        "INSERT INTO tree VALUES (6, 6)",
    ])
    .await;
    run(&mut s, "DELETE FROM tree WHERE id = 1").await;
    assert!(
        query(&mut s, "SELECT id FROM tree ORDER BY id").await
            == vec![text_row(&["5"]), text_row(&["6"])]
    );
    run(&mut s, "DELETE FROM tree WHERE id = 6").await;
    assert!(query(&mut s, "SELECT id FROM tree ORDER BY id").await == vec![text_row(&["5"])]);
}

/// The cycle again, entered at a row an earlier part of the same command
/// modified off-key. Reaching that row and stopping at it are two different
/// rules, and this is where they meet: the cascade must delete it — the outer
/// command only touched a non-key column — and must still terminate when the
/// cycle brings it back to what the cascade itself has already deleted.
#[tokio::test]
async fn a_cascade_cycle_entered_at_a_command_modified_row_terminates() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE cyc_a (id int4 PRIMARY KEY, b int4, note int4)",
        "CREATE TABLE cyc_b (id int4 PRIMARY KEY, a int4 REFERENCES cyc_a (id) ON DELETE CASCADE)",
        "ALTER TABLE cyc_a ADD CONSTRAINT cyc_a_b_fkey FOREIGN KEY (b) REFERENCES cyc_b (id) \
         ON DELETE CASCADE",
        "INSERT INTO cyc_a VALUES (1, NULL, 0), (2, NULL, 0)",
        "INSERT INTO cyc_b VALUES (10, 1), (20, 2)",
        "UPDATE cyc_a SET b = 10 WHERE id = 1",
        "UPDATE cyc_a SET b = 20 WHERE id = 2",
    ])
    .await;
    run(
        &mut s,
        "WITH u AS (UPDATE cyc_a SET note = 1 WHERE id = 1 RETURNING id) \
         DELETE FROM cyc_b WHERE id = 10",
    )
    .await;
    assert!(
        query(&mut s, "SELECT id, b, note FROM cyc_a ORDER BY id").await
            == vec![text_row(&["2", "20", "0"])]
    );
    assert!(
        query(&mut s, "SELECT id, a FROM cyc_b ORDER BY id").await == vec![text_row(&["20", "2"])]
    );
}

/// A cascade crosses as many relations as the chain has: grandparent to parent
/// to child, in one statement.
#[tokio::test]
async fn a_cascade_walks_a_multi_level_chain() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE gp (id int4 PRIMARY KEY)",
        "CREATE TABLE par (id int4 PRIMARY KEY, g int4 REFERENCES gp (id) ON DELETE CASCADE)",
        "CREATE TABLE ch (id int4 PRIMARY KEY, p int4 REFERENCES par (id) ON DELETE CASCADE)",
        "INSERT INTO gp VALUES (1), (2)",
        "INSERT INTO par VALUES (10, 1), (20, 2)",
        "INSERT INTO ch VALUES (100, 10), (200, 20)",
    ])
    .await;
    run(&mut s, "DELETE FROM gp WHERE id = 1").await;
    assert!(query(&mut s, "SELECT id FROM par ORDER BY id").await == vec![text_row(&["20"])]);
    assert!(query(&mut s, "SELECT id FROM ch ORDER BY id").await == vec![text_row(&["200"])]);
    run(&mut s, "DELETE FROM gp").await;
    assert!(query(&mut s, "SELECT id FROM par").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM ch").await == no_rows());
}

/// The rows a cascade deletes are themselves parents, and the checks they owe
/// run against a third relation. Where that relation's own action is `CASCADE`
/// the chain simply continues; where it is `NO ACTION` the whole statement is
/// refused, and the message names the *cascaded* relation, not the one the user
/// wrote.
#[tokio::test]
async fn a_cascade_fires_further_checks_on_a_third_table() {
    struct Case {
        third_action: &'static str,
        expect: Result<(), PgError>,
        why: &'static str,
    }
    let cases = [
        Case {
            third_action: "ON DELETE CASCADE",
            expect: Ok(()),
            why: "the third relation cascades too, and all three empty",
        },
        Case {
            third_action: "",
            expect: Err(PgError::error(
                "23503",
                "update or delete on table \"mid\" violates foreign key constraint \
                 \"leaf_m_fkey\" on table \"leaf\"",
            )
            .with_detail("Key (id)=(10) is still referenced from table \"leaf\".")),
            why: "the cascaded delete of mid's row is refused by leaf's NO ACTION",
        },
    ];
    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE top (id int4 PRIMARY KEY)",
            "CREATE TABLE mid (id int4 PRIMARY KEY, t int4 REFERENCES top (id) ON DELETE CASCADE)",
            &format!(
                "CREATE TABLE leaf (id int4 PRIMARY KEY, m int4 REFERENCES mid (id) {})",
                case.third_action
            ),
            "INSERT INTO top VALUES (1)",
            "INSERT INTO mid VALUES (10, 1)",
            "INSERT INTO leaf VALUES (100, 10)",
        ])
        .await;
        match case.expect {
            Ok(()) => {
                run(&mut s, "DELETE FROM top WHERE id = 1").await;
                assert!(
                    query(&mut s, "SELECT id FROM top").await == no_rows(),
                    "{}",
                    case.why
                );
                assert!(
                    query(&mut s, "SELECT id FROM mid").await == no_rows(),
                    "{}",
                    case.why
                );
                assert!(
                    query(&mut s, "SELECT id FROM leaf").await == no_rows(),
                    "{}",
                    case.why
                );
            }
            Err(expected) => {
                assert!(
                    error(&mut s, "DELETE FROM top WHERE id = 1").await == expected,
                    "{}",
                    case.why
                );
                // Nothing the cascade staged survives the refusal.
                assert!(query(&mut s, "SELECT id FROM top").await == vec![text_row(&["1"])]);
                assert!(query(&mut s, "SELECT id FROM mid").await == vec![text_row(&["10"])]);
                assert!(query(&mut s, "SELECT id FROM leaf").await == vec![text_row(&["100"])]);
            }
        }
    }
}
