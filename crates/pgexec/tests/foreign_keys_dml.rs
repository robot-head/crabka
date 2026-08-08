//! Child-side foreign key enforcement across every write path: `INSERT`,
//! `UPDATE`, `COPY FROM STDIN`, `MERGE` and `ON CONFLICT DO UPDATE`, plus the
//! statement-level timing that makes a self-referencing `NOT DEFERRABLE`
//! constraint satisfiable, `MATCH SIMPLE`/`MATCH FULL` null handling, permuted
//! composite key columns and cross-width integer keys.
//!
//! Every expectation here comes from a live `PostgreSQL` 18.4, and not from
//! documentation. That covers the SQLSTATE, the primary message and the
//! `DETAIL`. The oracle is `postgres:18`, which reports
//! `PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1)`.

use assert2::assert;
use bytes::Bytes;
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

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// The whole reportable shape of a failed statement, so a case compares one
/// value and not three fields. `DETAIL` is the part that names the offending
/// key, and it is the part an engine most easily drops.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
    detail: Option<String>,
}

impl From<PgError> for Failure {
    fn from(error: PgError) -> Self {
        let diagnostics = error.diagnostics.unwrap_or_default();
        Self {
            code: error.code,
            message: error.message,
            detail: diagnostics.detail,
        }
    }
}

async fn failure(s: &mut SqlSession, sql: &str) -> Failure {
    s.simple_query(sql)
        .await
        .expect_err("statement should violate the foreign key")
        .into()
}

/// `23503` as every child-side write path reports it. The primary message says
/// "insert or update" whatever the statement was, because `UPDATE`, `COPY`,
/// `MERGE` and `ON CONFLICT DO UPDATE` all reuse the `INSERT` wording. `DETAIL`
/// names the key in `FOREIGN KEY` clause order.
fn key_not_present(child: &str, constraint: &str, key: &str, parent: &str) -> Failure {
    Failure {
        code: "23503".to_string(),
        message: format!(
            "insert or update on table \"{child}\" violates foreign key constraint \"{constraint}\""
        ),
        detail: Some(format!("{key} is not present in table \"{parent}\".")),
    }
}

/// `MATCH FULL` refuses a mixed key before it probes anything, so its `DETAIL`
/// names no key at all.
fn match_full_mixed_nulls(child: &str, constraint: &str) -> Failure {
    Failure {
        code: "23503".to_string(),
        message: format!(
            "insert or update on table \"{child}\" violates foreign key constraint \"{constraint}\""
        ),
        detail: Some(
            "MATCH FULL does not allow mixing of null and nonnull key values.".to_string(),
        ),
    }
}

// ---------------------------------------------------------------------------
// The child side, from every write path

/// A parent with one key, and a child holding one row that references it.
const CHILD_SETUP: &[&str] = &[
    "CREATE TABLE p (id int4 PRIMARY KEY)",
    "INSERT INTO p VALUES (10)",
    "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id))",
    "INSERT INTO c VALUES (1, 10)",
];

/// Every statement that can write a referencing key must run the same check and
/// report the same `23503`, down to the `DETAIL` line. `COPY` has its own test
/// because it enters through the copy-in protocol, not through a simple query.
#[tokio::test]
async fn child_violation_from_every_simple_query_write_path() {
    struct Case {
        sql: &'static str,
        why: &'static str,
    }
    let cases = [
        Case {
            sql: "INSERT INTO c VALUES (2, 1)",
            why: "a fresh referencing row",
        },
        Case {
            sql: "INSERT INTO c SELECT 2, 1",
            why: "INSERT ... SELECT writes through the same path",
        },
        Case {
            sql: "UPDATE c SET a = 1 WHERE id = 1",
            why: "an UPDATE that moves the key off its parent",
        },
        Case {
            sql: "MERGE INTO c USING (SELECT 2 AS k) s ON c.id = s.k \
                  WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.k, 1)",
            why: "MERGE's NOT MATCHED insert",
        },
        Case {
            sql: "MERGE INTO c USING (SELECT 1 AS k) s ON c.id = s.k \
                  WHEN MATCHED THEN UPDATE SET a = 1",
            why: "MERGE's MATCHED update",
        },
        Case {
            sql: "INSERT INTO c VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET a = 1",
            why: "the DO UPDATE arm writes a key the excluded row never carried",
        },
    ];
    let expected = key_not_present("c", "c_a_fkey", "Key (a)=(1)", "p");
    for case in cases {
        let (_engine, mut s) = engine_with(CHILD_SETUP).await;
        let got = failure(&mut s, case.sql).await;
        assert!(got == expected, "{}: {}", case.why, case.sql);
        // The statement is atomic: nothing it wrote survives the violation.
        assert!(query(&mut s, "SELECT id, a FROM c ORDER BY id").await == [text_row(&["1", "10"])]);
    }
}

/// A row that violates two of the child's foreign keys at once reports the one
/// declared first, whatever the two are called.
///
/// `PostgreSQL` fires the child-side triggers in `pg_constraint.oid` order, so
/// the constraint written first raises the `23503` and the second never runs.
/// Each pair below is written in both name orders, so nothing here passes by
/// the names happening to sort the right way.
#[tokio::test]
async fn the_first_declared_child_side_constraint_reports_the_violation() {
    for (first, second) in [("zz", "aa"), ("aa", "zz")] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p1 (id int4 PRIMARY KEY)",
            "CREATE TABLE p2 (id int4 PRIMARY KEY)",
            &format!(
                "CREATE TABLE c (id int4 PRIMARY KEY, a int4, \
                 CONSTRAINT {first} FOREIGN KEY (a) REFERENCES p1 (id), \
                 CONSTRAINT {second} FOREIGN KEY (a) REFERENCES p2 (id))"
            ),
        ])
        .await;
        assert!(
            failure(&mut s, "INSERT INTO c VALUES (1, 99)").await
                == key_not_present("c", first, "Key (a)=(99)", "p1"),
            "the constraint declared first reports, not the one named first"
        );
    }
}

/// `COPY FROM STDIN` reaches the write path through the copy-in protocol, which
/// bypasses simple-query statement dispatch entirely. So the drain has to be
/// wired into it separately, and the failed copy must leave no rows.
#[tokio::test]
async fn copy_from_stdin_reports_the_child_violation() {
    let (_engine, mut s) = engine_with(CHILD_SETUP).await;

    let error = s
        .copy_in(
            "COPY c (id, a) FROM STDIN",
            vec![Bytes::from_static(b"2\t1\n")],
        )
        .await
        .expect_err("COPY should violate the foreign key");
    assert!(Failure::from(error) == key_not_present("c", "c_a_fkey", "Key (a)=(1)", "p"));
    assert!(query(&mut s, "SELECT id, a FROM c ORDER BY id").await == [text_row(&["1", "10"])]);

    // A COPY whose keys are all present still lands, so the check is not simply
    // refusing every copied row.
    s.copy_in(
        "COPY c (id, a) FROM STDIN",
        vec![Bytes::from_static(b"3\t10\n4\t\\N\n")],
    )
    .await
    .expect("COPY of satisfied keys should succeed");
    assert!(
        query(&mut s, "SELECT id, a FROM c ORDER BY id").await
            == [
                text_row(&["1", "10"]),
                text_row(&["3", "10"]),
                row(&[Some("4"), None]),
            ]
    );
}

// ---------------------------------------------------------------------------
// Timing: the check runs after the statement's rows exist

/// The load-bearing timing fact. `PostgreSQL` implements referential integrity
/// as an `AFTER ROW` trigger, so a row may satisfy a `NOT DEFERRABLE`
/// self-referencing foreign key with *itself*, because the row exists by the
/// time the check runs. An engine that probes inline, at the moment it writes
/// the row, fails both of these with a spurious `23503`.
#[tokio::test]
async fn not_deferrable_self_reference_is_satisfied_by_the_statement_itself() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE t (id int4 PRIMARY KEY, boss int4 REFERENCES t (id))"]).await;

    // A row that is its own parent.
    run(&mut s, "INSERT INTO t (id, boss) VALUES (1, 1)").await;
    // And a multi-row INSERT whose second row's parent is its first row: the
    // check point is the statement, not the row.
    run(&mut s, "INSERT INTO t (id, boss) VALUES (2, NULL), (3, 2)").await;

    assert!(
        query(&mut s, "SELECT id, boss FROM t ORDER BY id").await
            == [
                text_row(&["1", "1"]),
                row(&[Some("2"), None]),
                text_row(&["3", "2"]),
            ]
    );

    // The constraint is still enforced — the drain runs, it just runs late.
    assert!(
        failure(&mut s, "INSERT INTO t (id, boss) VALUES (4, 99)").await
            == key_not_present("t", "t_boss_fkey", "Key (boss)=(99)", "t")
    );
}

/// A `WITH` item and the body it feeds are one command, so the drain fires once
/// at the end of the whole statement, not once per part. Both orderings prove
/// it. The parent the `WITH` item writes satisfies the body, *and* a parent the
/// body has not yet written satisfies the child the `WITH` item writes.
#[tokio::test]
async fn with_item_and_body_drain_once_for_the_whole_statement() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE wp (id int4 PRIMARY KEY)",
        "CREATE TABLE wc (a int4 REFERENCES wp (id))",
    ])
    .await;

    run(
        &mut s,
        "WITH ins AS (INSERT INTO wp VALUES (5) RETURNING id) INSERT INTO wc SELECT id FROM ins",
    )
    .await;
    assert!(query(&mut s, "SELECT a FROM wc ORDER BY a").await == [text_row(&["5"])]);

    run(
        &mut s,
        "WITH ins AS (INSERT INTO wc VALUES (6) RETURNING a) INSERT INTO wp SELECT a FROM ins",
    )
    .await;
    assert!(
        query(&mut s, "SELECT a FROM wc ORDER BY a").await == [text_row(&["5"]), text_row(&["6"])]
    );

    // Nothing in the statement supplies the parent, so the same late drain
    // reports the violation.
    assert!(
        failure(
            &mut s,
            "WITH ins AS (INSERT INTO wc VALUES (7) RETURNING a) SELECT a FROM ins",
        )
        .await
            == key_not_present("wc", "wc_a_fkey", "Key (a)=(7)", "wp")
    );
    assert!(
        query(&mut s, "SELECT a FROM wc ORDER BY a").await == [text_row(&["5"]), text_row(&["6"])]
    );
}

/// A row `ON CONFLICT DO NOTHING` skips never becomes a row, so it owes no
/// check. Nothing probes the key it carried, even though no parent holds it.
#[tokio::test]
async fn on_conflict_do_nothing_queues_no_check_for_a_skipped_row() {
    let (_engine, mut s) = engine_with(CHILD_SETUP).await;

    run(
        &mut s,
        "INSERT INTO c VALUES (1, 999) ON CONFLICT (id) DO NOTHING",
    )
    .await;
    assert!(query(&mut s, "SELECT id, a FROM c ORDER BY id").await == [text_row(&["1", "10"])]);

    // The same statement against a non-conflicting id does insert, and then the
    // check does run.
    assert!(
        failure(
            &mut s,
            "INSERT INTO c VALUES (2, 999) ON CONFLICT (id) DO NOTHING",
        )
        .await
            == key_not_present("c", "c_a_fkey", "Key (a)=(999)", "p")
    );
}

// ---------------------------------------------------------------------------
// MATCH semantics

/// `MATCH SIMPLE`, the default, lets any NULL in the key through with no probe,
/// including a partial NULL in a composite key. `MATCH FULL` accepts an all-NULL
/// key and rejects a mixed one with a `DETAIL` that names no key.
#[tokio::test]
async fn match_simple_and_match_full_null_handling() {
    struct Case {
        sql: &'static str,
        expect: Option<Failure>,
        why: &'static str,
    }
    let cases = [
        Case {
            sql: "INSERT INTO csimple VALUES (99, NULL)",
            expect: None,
            why: "MATCH SIMPLE: a partial NULL passes without probing",
        },
        Case {
            sql: "INSERT INTO csimple VALUES (NULL, 99)",
            expect: None,
            why: "MATCH SIMPLE: which column is NULL does not matter",
        },
        Case {
            sql: "INSERT INTO csimple VALUES (NULL, NULL)",
            expect: None,
            why: "MATCH SIMPLE: an all-NULL key passes",
        },
        Case {
            sql: "INSERT INTO csimple VALUES (1, 2)",
            expect: None,
            why: "MATCH SIMPLE: a fully present key probes and finds its parent",
        },
        Case {
            sql: "INSERT INTO csimple VALUES (1, 99)",
            expect: Some(key_not_present(
                "csimple",
                "csimple_a_b_fkey",
                "Key (a, b)=(1, 99)",
                "p",
            )),
            why: "MATCH SIMPLE: a fully present key that is absent is a violation",
        },
        Case {
            sql: "INSERT INTO cfull VALUES (NULL, NULL)",
            expect: None,
            why: "MATCH FULL: an all-NULL key passes",
        },
        Case {
            sql: "INSERT INTO cfull VALUES (1, NULL)",
            expect: Some(match_full_mixed_nulls("cfull", "cfull_a_b_fkey")),
            why: "MATCH FULL: a mixed key is refused before any probe",
        },
        Case {
            sql: "INSERT INTO cfull VALUES (NULL, 2)",
            expect: Some(match_full_mixed_nulls("cfull", "cfull_a_b_fkey")),
            why: "MATCH FULL: the mixed-null refusal is column-order blind",
        },
        Case {
            sql: "INSERT INTO cfull VALUES (1, 2)",
            expect: None,
            why: "MATCH FULL: a fully present key still probes normally",
        },
        Case {
            sql: "INSERT INTO cfull VALUES (1, 99)",
            expect: Some(key_not_present(
                "cfull",
                "cfull_a_b_fkey",
                "Key (a, b)=(1, 99)",
                "p",
            )),
            why: "MATCH FULL: an absent non-NULL key reports the ordinary DETAIL",
        },
    ];

    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (x int4, y int4, PRIMARY KEY (x, y))",
        "INSERT INTO p VALUES (1, 2)",
        "CREATE TABLE csimple (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p (x, y))",
        "CREATE TABLE cfull (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p (x, y) MATCH FULL)",
    ])
    .await;

    for case in cases {
        match case.expect {
            None => {
                s.simple_query(case.sql)
                    .await
                    .unwrap_or_else(|e| panic!("{}: {} failed with {e}", case.why, case.sql));
            }
            Some(expected) => {
                let got = failure(&mut s, case.sql).await;
                assert!(got == expected, "{}: {}", case.why, case.sql);
            }
        }
    }
}

/// A single-column NULL key is satisfied on the way in and on the way out. An
/// `INSERT` of NULL never probes, and an `UPDATE` that clears a key to NULL
/// never probes either.
#[tokio::test]
async fn null_child_key_is_always_satisfied() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "INSERT INTO p VALUES (10)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id))",
    ])
    .await;

    run(&mut s, "INSERT INTO c VALUES (1, NULL)").await;
    run(&mut s, "INSERT INTO c VALUES (2, 10)").await;
    run(&mut s, "UPDATE c SET a = NULL WHERE id = 2").await;
    assert!(
        query(&mut s, "SELECT id, a FROM c ORDER BY id").await
            == [row(&[Some("1"), None]), row(&[Some("2"), None])]
    );
}

// ---------------------------------------------------------------------------
// An UPDATE that leaves the key alone

/// `PostgreSQL`'s child-side trigger compares the old and new keys and returns
/// with no probe when they are equal, so a row that already violates the
/// constraint survives an unrelated column update. `NOT VALID` is what makes
/// that observable from SQL alone. It admits a violating row into storage, and
/// the constraint still governs every subsequent write. Without the skip, the
/// `note` update below would re-probe the untouched key and fail.
#[tokio::test]
async fn update_that_leaves_the_key_unchanged_is_not_rechecked() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE nvp (id int4 PRIMARY KEY)",
        "CREATE TABLE nvc (a int4, note text)",
        "INSERT INTO nvc VALUES (7, 'old')",
        "ALTER TABLE nvc ADD CONSTRAINT nv_fk FOREIGN KEY (a) REFERENCES nvp (id) NOT VALID",
    ])
    .await;

    // A non-key column moves; the key does not, so no check is queued.
    run(&mut s, "UPDATE nvc SET note = 'new'").await;
    // Assigning the key its own value is still "unchanged" — the comparison is
    // on values, not on whether the column appeared in the SET list.
    run(&mut s, "UPDATE nvc SET a = 7").await;
    assert!(query(&mut s, "SELECT a, note FROM nvc").await == [text_row(&["7", "new"])]);

    // Moving the key to a different absent value does queue a check.
    assert!(
        failure(&mut s, "UPDATE nvc SET a = 8").await
            == key_not_present("nvc", "nv_fk", "Key (a)=(8)", "nvp")
    );
    // And a fresh row is checked regardless of NOT VALID.
    assert!(
        failure(&mut s, "INSERT INTO nvc VALUES (9, 'fresh')").await
            == key_not_present("nvc", "nv_fk", "Key (a)=(9)", "nvp")
    );
    assert!(query(&mut s, "SELECT a, note FROM nvc").await == [text_row(&["7", "new"])]);
}

// ---------------------------------------------------------------------------
// Composite keys whose column order differs from the referenced index

/// `FOREIGN KEY (b, a) REFERENCES pperm (y, x)` over a `(x, y)` primary key
/// pairs the two lists positionally: `b` matches `y` and `a` matches `x`. The
/// probe must permute the child's values into the index's order, and the
/// `DETAIL` still names the columns in `FOREIGN KEY` clause order.
#[tokio::test]
async fn composite_foreign_key_with_permuted_column_order() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE pperm (x int4, y int4, PRIMARY KEY (x, y))",
        "INSERT INTO pperm VALUES (1, 2)",
        "CREATE TABLE cperm (a int4, b int4, FOREIGN KEY (b, a) REFERENCES pperm (y, x))",
    ])
    .await;

    // (b, a) = (2, 1) pairs with (y, x) = (2, 1), which is the stored (x, y) = (1, 2).
    run(&mut s, "INSERT INTO cperm (a, b) VALUES (1, 2)").await;
    // (b, a) = (1, 2) pairs with (y, x) = (1, 2), i.e. (x, y) = (2, 1), which is absent.
    // An engine that probed without permuting would accept this row and reject the one above.
    assert!(
        failure(&mut s, "INSERT INTO cperm (a, b) VALUES (2, 1)").await
            == key_not_present("cperm", "cperm_b_a_fkey", "Key (b, a)=(1, 2)", "pperm")
    );
    assert!(query(&mut s, "SELECT a, b FROM cperm").await == [text_row(&["1", "2"])]);
}

// ---------------------------------------------------------------------------
// Keys whose two sides are different integer widths

/// `int2`/`int4`/`int8` share an operator family, so a foreign key may pair them.
/// The probe must widen or narrow the child's value, and must not compare raw
/// encodings. A value representable on both sides resolves. A value
/// that is not present in the parent still reports its own literal in
/// `DETAIL`.
#[tokio::test]
async fn cross_width_integer_keys_resolve() {
    struct Case {
        setup: &'static [&'static str],
        accepted: &'static str,
        rejected: &'static str,
        expect: Failure,
        surviving: &'static [&'static str],
        why: &'static str,
    }
    let cases = [
        Case {
            setup: &[
                "CREATE TABLE p8 (id int8 PRIMARY KEY)",
                "INSERT INTO p8 VALUES (4294967296), (5)",
                "CREATE TABLE c4 (a int4 REFERENCES p8 (id))",
            ],
            accepted: "INSERT INTO c4 VALUES (5)",
            rejected: "INSERT INTO c4 VALUES (6)",
            expect: key_not_present("c4", "c4_a_fkey", "Key (a)=(6)", "p8"),
            surviving: &["5"],
            why: "an int4 child column referencing an int8 parent key",
        },
        Case {
            setup: &[
                "CREATE TABLE p4 (id int4 PRIMARY KEY)",
                "INSERT INTO p4 VALUES (5)",
                "CREATE TABLE c8 (a int8 REFERENCES p4 (id))",
            ],
            accepted: "INSERT INTO c8 VALUES (5)",
            // Beyond int4's range entirely, so no parent key can hold it.
            rejected: "INSERT INTO c8 VALUES (4294967296)",
            expect: key_not_present("c8", "c8_a_fkey", "Key (a)=(4294967296)", "p4"),
            surviving: &["5"],
            why: "an int8 child column referencing an int4 parent key",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(case.setup).await;
        s.simple_query(case.accepted)
            .await
            .unwrap_or_else(|e| panic!("{}: {} failed with {e}", case.why, case.accepted));
        let got = failure(&mut s, case.rejected).await;
        assert!(got == case.expect, "{}: {}", case.why, case.rejected);

        let table = case.accepted.split_whitespace().nth(2).expect("table name");
        let expected: Vec<Vec<Option<String>>> = case
            .surviving
            .iter()
            .map(|value| text_row(&[value]))
            .collect();
        assert!(
            query(&mut s, &format!("SELECT a FROM {table} ORDER BY a")).await == expected,
            "{}",
            case.why
        );
    }
}
