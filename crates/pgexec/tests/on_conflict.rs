//! `INSERT … ON CONFLICT` semantics: arbiter resolution, `DO NOTHING`,
//! `DO UPDATE` with `excluded`, action `WHERE` and `RETURNING`, command tags,
//! intra-statement conflicts, transaction interaction, and the SQLSTATEs of
//! every refused conflict target.
//!
//! NOTE: this file is deliberately named `on_conflict.rs`. A test target whose
//! name contains the substring `update` trips Windows UAC installer detection,
//! which is os error 740. See the note at the top of `mutation_semantics.rs`.

use assert2::assert;
use bytes::Bytes;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{BoundParam, Cell, Engine, ExecuteOutcome, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("statement should succeed")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
        o @ QueryResult::Empty => panic!("expected a tagged result, got {o:?}"),
    }
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

/// Run `sql`, a single statement, and return its command tag.
async fn tag(s: &mut SqlSession, sql: &str) -> String {
    tag_of(&run(s, sql).await[0]).to_string()
}

/// Run `sql`, a single statement, and return its result rows as text.
async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

async fn err_code(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql).await.expect_err("expected error").code
}

/// A fresh engine with `setup` applied, plus one connected session. This
/// function returns the engine so the caller keeps it alive for the session's
/// lifetime.
async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

// ---------------------------------------------------------------------------
// DO NOTHING
// ---------------------------------------------------------------------------

/// A bare `ON CONFLICT DO NOTHING`, with no conflict target, arbitrates over
/// every unique index. An explicit column target arbitrates the matching one.
/// Both skip the conflicting row, insert the rest, and count only the inserts.
#[tokio::test]
async fn do_nothing_skips_conflicting_row_with_and_without_a_target() {
    for target in ["", "(id)", "ON CONSTRAINT t_pkey"] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
            "INSERT INTO t VALUES (1, 'a')",
        ])
        .await;

        let sql =
            format!("INSERT INTO t VALUES (1, 'x'), (2, 'b') ON CONFLICT {target} DO NOTHING");
        assert!(tag(&mut s, &sql).await == "INSERT 0 1", "target {target:?}");
        assert!(
            query(&mut s, "SELECT id, v FROM t ORDER BY id").await
                == vec![row(&["1", "a"]), row(&["2", "b"])],
            "target {target:?}"
        );
    }
}

/// Two `VALUES` rows carry the same new key. The first inserts, and the second
/// conflicts with a key that exists only in this statement's pending batch.
#[tokio::test]
async fn do_nothing_skips_an_intra_statement_duplicate() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4 PRIMARY KEY, v text)"]).await;

    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (1, 'first'), (1, 'second') ON CONFLICT (id) DO NOTHING"
        )
        .await
            == "INSERT 0 1"
    );
    assert!(query(&mut s, "SELECT id, v FROM t").await == vec![row(&["1", "first"])]);
}

/// The arbiter only excuses conflicts on ITS index. A duplicate on a different
/// unique index is still a plain 23505.
#[tokio::test]
async fn conflict_on_a_non_arbiter_unique_index_is_still_23505() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text UNIQUE)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    assert!(
        err_code(
            &mut s,
            "INSERT INTO t VALUES (2, 'a') ON CONFLICT (id) DO NOTHING"
        )
        .await
            == "23505"
    );
    assert!(
        err_code(
            &mut s,
            "INSERT INTO t VALUES (2, 'a') ON CONFLICT (id) DO UPDATE SET v = excluded.v"
        )
        .await
            == "23505"
    );
}

/// `DO NOTHING` on a table with no unique index at all. Nothing can ever
/// arbitrate, so every row inserts. An empty arbiter set is not an error.
#[tokio::test]
async fn do_nothing_on_a_table_without_unique_indexes_inserts_every_row() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4, v text)"]).await;

    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (1, 'a'), (1, 'b') ON CONFLICT DO NOTHING"
        )
        .await
            == "INSERT 0 2"
    );
    assert!(query(&mut s, "SELECT count(*) FROM t").await == vec![row(&["2"])]);
}

/// The engine checks NOT NULL on the proposed row before arbitration, exactly
/// as Postgres does. `DO NOTHING` never excuses a 23502.
#[tokio::test]
async fn not_null_still_fires_under_do_nothing() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text NOT NULL)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    // Even the row that WOULD be skipped raises 23502.
    assert!(
        err_code(
            &mut s,
            "INSERT INTO t VALUES (1, NULL) ON CONFLICT (id) DO NOTHING"
        )
        .await
            == "23502"
    );
}

/// SQL unique indexes treat NULLs as distinct, so a NULL in an arbiter key can
/// never conflict. Repeated NULL rows all insert.
#[tokio::test]
async fn null_in_the_arbiter_key_never_conflicts() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (v text UNIQUE, n int4)"]).await;

    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (NULL, 1), (NULL, 2) ON CONFLICT (v) DO NOTHING"
        )
        .await
            == "INSERT 0 2"
    );
    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (NULL, 3) ON CONFLICT (v) DO UPDATE SET n = excluded.n"
        )
        .await
            == "INSERT 0 1"
    );
    assert!(query(&mut s, "SELECT count(*) FROM t").await == vec![row(&["3"])]);
}

/// `RETURNING` reports exactly the rows the statement inserted. A skipped row
/// produces no output row.
#[tokio::test]
async fn do_nothing_returning_reports_only_inserted_rows() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    let r = &run(
        &mut s,
        "INSERT INTO t VALUES (1, 'x'), (2, 'b'), (3, 'c') \
         ON CONFLICT (id) DO NOTHING RETURNING id, v",
    )
    .await[0];
    assert!(tag_of(r) == "INSERT 0 2");
    assert!(rows_text(r) == vec![row(&["2", "b"]), row(&["3", "c"])]);
}

// ---------------------------------------------------------------------------
// DO UPDATE
// ---------------------------------------------------------------------------

/// The core upsert. The statement updates the conflicting row from `excluded`,
/// inserts the non-conflicting row, and the tag counts BOTH.
#[tokio::test]
async fn do_update_upserts_and_counts_inserted_plus_updated() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text, n int4)",
        "INSERT INTO t VALUES (1, 'a', 10)",
    ])
    .await;

    let r = &run(
        &mut s,
        "INSERT INTO t VALUES (1, 'x', 99), (2, 'b', 20) \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v, n = t.n + excluded.n \
         RETURNING id, v, n",
    )
    .await[0];
    assert!(tag_of(r) == "INSERT 0 2");
    assert!(rows_text(r) == vec![row(&["1", "x", "109"]), row(&["2", "b", "20"])]);
    assert!(
        query(&mut s, "SELECT id, v, n FROM t ORDER BY id").await
            == vec![row(&["1", "x", "109"]), row(&["2", "b", "20"])]
    );
}

/// A `DO UPDATE … WHERE` that is not true leaves the stored row untouched, is
/// not inserted, produces no `RETURNING` row, and does not count.
#[tokio::test]
async fn do_update_with_a_false_action_where_touches_nothing() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    let r = &run(
        &mut s,
        "INSERT INTO t VALUES (1, 'x') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE false RETURNING id, v",
    )
    .await[0];
    assert!(tag_of(r) == "INSERT 0 0");
    assert!(rows_text(r).is_empty());
    assert!(query(&mut s, "SELECT id, v FROM t").await == vec![row(&["1", "a"])]);

    // A predicate over the stored row and `excluded` gates the update per row.
    let r = &run(
        &mut s,
        "INSERT INTO t VALUES (1, 'x'), (2, 'b') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE t.v <> 'a' RETURNING id, v",
    )
    .await[0];
    assert!(tag_of(r) == "INSERT 0 1");
    assert!(rows_text(r) == vec![row(&["2", "b"])]);
    assert!(
        query(&mut s, "SELECT id, v FROM t ORDER BY id").await
            == vec![row(&["1", "a"]), row(&["2", "b"])]
    );
}

/// `SET` right-hand sides must qualify a column that exists in BOTH the target
/// and `excluded` scopes. A bare duplicate name is ambiguous, which is 42702,
/// exactly as in Postgres. Both qualifications work.
#[tokio::test]
async fn do_update_set_expression_scopes() {
    struct Case {
        assignment: &'static str,
        expected: Option<&'static str>,
    }
    let cases = [
        Case {
            assignment: "v = v",
            expected: None, // ambiguous → 42702
        },
        Case {
            assignment: "v = t.v || '!'",
            expected: Some("a!"),
        },
        Case {
            assignment: "v = excluded.v",
            expected: Some("x"),
        },
        Case {
            assignment: "v = excluded.v || t.v",
            expected: Some("xa"),
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
            "INSERT INTO t VALUES (1, 'a')",
        ])
        .await;
        let sql = format!(
            "INSERT INTO t VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET {}",
            case.assignment
        );
        match case.expected {
            None => assert!(
                err_code(&mut s, &sql).await == "42702",
                "{}",
                case.assignment
            ),
            Some(expected) => {
                assert!(
                    tag(&mut s, &sql).await == "INSERT 0 1",
                    "{}",
                    case.assignment
                );
                assert!(
                    query(&mut s, "SELECT v FROM t").await == vec![row(&[expected])],
                    "{}",
                    case.assignment
                );
            }
        }
    }
}

/// `excluded` is not in scope in `RETURNING`. Postgres raises 42P01 for the
/// missing FROM-clause entry.
#[tokio::test]
#[expect(non_snake_case, reason = "SQLSTATE in the test name")]
async fn returning_excluded_is_42P01() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    assert!(
        err_code(
            &mut s,
            "INSERT INTO t VALUES (1, 'x') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v RETURNING excluded.v"
        )
        .await
            == "42P01"
    );
}

/// A `DO UPDATE` may only touch a given row once per statement. That holds both
/// when the duplicate key exists only in this statement, as in
/// `VALUES (1), (1)`, and when two `VALUES` rows conflict onto the same STORED
/// row. Both are 21000.
#[tokio::test]
async fn do_update_affecting_a_row_twice_is_21000() {
    struct Case {
        seed: Option<&'static str>,
        sql: &'static str,
    }
    let cases = [
        Case {
            seed: None,
            sql: "INSERT INTO t VALUES (1, 'a'), (1, 'b') \
                  ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        },
        Case {
            seed: Some("INSERT INTO t VALUES (1, 'seed')"),
            sql: "INSERT INTO t VALUES (1, 'a'), (1, 'b') \
                  ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        },
    ];

    for case in cases {
        let mut setup: Vec<&str> = vec!["CREATE TABLE t (id int4 PRIMARY KEY, v text)"];
        setup.extend(case.seed);
        let (_engine, mut s) = engine_with(&setup).await;
        assert!(err_code(&mut s, case.sql).await == "21000", "{}", case.sql);
        // The failed statement leaves nothing behind.
        let expected: Vec<Vec<Option<String>>> = case
            .seed
            .map(|_| vec![row(&["1", "seed"])])
            .unwrap_or_default();
        assert!(
            query(&mut s, "SELECT id, v FROM t ORDER BY id").await == expected,
            "{}",
            case.sql
        );
    }
}

/// A `DO UPDATE` is free to change the arbiter key itself. The row keeps its
/// identity but moves to a new key, and the old key becomes insertable.
#[tokio::test]
async fn do_update_may_change_the_arbiter_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET id = 7, v = excluded.v"
        )
        .await
            == "INSERT 0 1"
    );
    assert!(query(&mut s, "SELECT id, v FROM t ORDER BY id").await == vec![row(&["7", "x"])]);

    // Key 1 is free again.
    assert!(tag(&mut s, "INSERT INTO t VALUES (1, 'again')").await == "INSERT 0 1");
    assert!(
        query(&mut s, "SELECT id, v FROM t ORDER BY id").await
            == vec![row(&["1", "again"]), row(&["7", "x"])]
    );
}

/// A `DO UPDATE` whose post-image collides on a DIFFERENT unique index is a
/// plain 23505. The arbiter excuses only its own key.
#[tokio::test]
async fn do_update_tripping_another_unique_index_is_23505() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text UNIQUE)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
    ])
    .await;

    assert!(
        err_code(
            &mut s,
            "INSERT INTO t VALUES (1, 'zzz') ON CONFLICT (id) DO UPDATE SET v = 'b'"
        )
        .await
            == "23505"
    );
    assert!(
        query(&mut s, "SELECT id, v FROM t ORDER BY id").await
            == vec![row(&["1", "a"]), row(&["2", "b"])]
    );
}

/// ROLLBACK undoes an upsert inside an explicit transaction. The stored row
/// returns to its pre-statement image, and the inserted row disappears.
#[tokio::test]
async fn upsert_is_rolled_back_with_its_transaction() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'a')",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (1, 'x'), (2, 'b') ON CONFLICT (id) DO UPDATE SET v = excluded.v"
        )
        .await
            == "INSERT 0 2"
    );
    assert!(
        query(&mut s, "SELECT id, v FROM t ORDER BY id").await
            == vec![row(&["1", "x"]), row(&["2", "b"])]
    );
    run(&mut s, "ROLLBACK").await;
    assert!(query(&mut s, "SELECT id, v FROM t ORDER BY id").await == vec![row(&["1", "a"])]);
}

/// An upsert onto a row this same transaction inserted. The uncommitted row is
/// visible to its own xid, so the conflict is real and the update overwrites it
/// in place. Columns the `SET` list does not mention keep the first insert's
/// values, and the engine stores the row exactly once.
#[tokio::test]
async fn upsert_onto_a_row_inserted_by_the_same_transaction() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE t (id int4 PRIMARY KEY, v text, n int4)"]).await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "INSERT INTO t VALUES (1, 'first', 10)").await;
    assert!(
        tag(
            &mut s,
            "INSERT INTO t VALUES (1, 'second', 20) ON CONFLICT (id) DO UPDATE SET v = excluded.v"
        )
        .await
            == "INSERT 0 1"
    );
    // `n` was never assigned, so it keeps the first insert's value.
    assert!(query(&mut s, "SELECT id, v, n FROM t").await == vec![row(&["1", "second", "10"])]);
    run(&mut s, "COMMIT").await;
    assert!(query(&mut s, "SELECT id, v, n FROM t").await == vec![row(&["1", "second", "10"])]);
}

/// A delete of a row and an upsert of its key in the same transaction. The
/// deleted row is invisible to the arbiter, so the statement INSERTS a fresh row
/// rather than resurrecting the old one.
#[tokio::test]
async fn delete_then_upsert_in_one_transaction_inserts() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text, n int4)",
        "INSERT INTO t VALUES (1, 'a', 10)",
    ])
    .await;

    run(&mut s, "BEGIN").await;
    run(&mut s, "DELETE FROM t WHERE id = 1").await;
    assert!(
        tag(
            &mut s,
            "INSERT INTO t (id, v) VALUES (1, 'fresh') ON CONFLICT (id) DO UPDATE SET v = 'upd'"
        )
        .await
            == "INSERT 0 1"
    );
    // A fresh insert: `n` defaults to NULL instead of keeping the old row's 10.
    assert!(
        query(&mut s, "SELECT id, v, n FROM t").await
            == vec![vec![Some("1".into()), Some("fresh".into()), None]]
    );
    run(&mut s, "COMMIT").await;
    assert!(
        query(&mut s, "SELECT id, v, n FROM t").await
            == vec![vec![Some("1".into()), Some("fresh".into()), None]]
    );
}

// ---------------------------------------------------------------------------
// Conflict targets and their errors
// ---------------------------------------------------------------------------

/// `ON CONSTRAINT` resolves both constraint-backed index names, and column
/// inference is order-insensitive against a multi-column unique index.
#[tokio::test]
async fn conflict_targets_that_resolve() {
    struct Case {
        target: &'static str,
        why: &'static str,
    }
    let cases = [
        Case {
            target: "ON CONSTRAINT t_pkey",
            why: "primary-key constraint by name",
        },
        Case {
            target: "ON CONSTRAINT t_a_b_key",
            why: "unique constraint by generated name",
        },
        Case {
            target: "(a, b)",
            why: "column inference in index order",
        },
        Case {
            target: "(b, a)",
            why: "column inference is order-insensitive",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE t (id int4 PRIMARY KEY, a int4 NOT NULL, b int4 NOT NULL, \
             v text, UNIQUE (a, b))",
            "INSERT INTO t VALUES (1, 10, 20, 'seed')",
        ])
        .await;
        // Conflicts on whichever index the target names: id=1 for the pkey
        // targets, (a,b)=(10,20) for the unique-constraint targets.
        let sql = format!(
            "INSERT INTO t VALUES (1, 10, 20, 'x') ON CONFLICT {} DO UPDATE SET v = excluded.v",
            case.target
        );
        assert!(tag(&mut s, &sql).await == "INSERT 0 1", "{}", case.why);
        assert!(
            query(&mut s, "SELECT id, a, b, v FROM t").await == vec![row(&["1", "10", "20", "x"])],
            "{}",
            case.why
        );
    }
}

/// Every refused conflict target, with its SQLSTATE. Arbiter resolution runs
/// before the row loop, so these fire even though the single `VALUES` row would
/// not have conflicted with anything.
#[tokio::test]
async fn refused_conflict_targets_and_their_sqlstates() {
    struct Case {
        sql: &'static str,
        code: &'static str,
        why: &'static str,
    }
    let cases = [
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT (v) DO NOTHING",
            code: "42P10",
            why: "no unique index over exactly (v)",
        },
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT (id, v) DO NOTHING",
            code: "42P10",
            why: "column set is a superset of the pkey",
        },
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT (nosuch) DO NOTHING",
            code: "42703",
            why: "inference names a column that does not exist",
        },
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT ON CONSTRAINT nosuch DO NOTHING",
            code: "42704",
            why: "unknown constraint name",
        },
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT ON CONSTRAINT t_v_idx DO NOTHING",
            code: "42704",
            why: "a plain index is not a constraint",
        },
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT DO UPDATE SET v = excluded.v",
            code: "42601",
            why: "DO UPDATE requires a conflict target",
        },
        Case {
            sql: "INSERT INTO t VALUES (99, 'z') ON CONFLICT (id) WHERE id > 0 DO NOTHING",
            code: "0A000",
            why: "inference predicates need partial indexes",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE t (id int4 PRIMARY KEY, v text)",
            "CREATE INDEX t_v_idx ON t (v)",
            "INSERT INTO t VALUES (1, 'a')",
        ])
        .await;
        assert!(
            err_code(&mut s, case.sql).await == case.code,
            "{}",
            case.why
        );
        // Nothing was written: the target is resolved before the row loop.
        assert!(
            query(&mut s, "SELECT id, v FROM t ORDER BY id").await == vec![row(&["1", "a"])],
            "{}",
            case.why
        );
    }
}

/// A sharded table's unique keys live on other ranges, so `ON CONFLICT` cannot
/// arbitrate there. The engine refuses it permanently with 0A000, as it refuses
/// `RETURNING`.
#[tokio::test]
async fn on_conflict_on_a_sharded_table_is_0a000() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE s (id int4 NOT NULL, v text) SHARDED"]).await;

    assert!(
        err_code(
            &mut s,
            "INSERT INTO s VALUES (1, 'a') ON CONFLICT DO NOTHING"
        )
        .await
            == "0A000"
    );
    assert!(
        err_code(
            &mut s,
            "INSERT INTO s VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = excluded.v"
        )
        .await
            == "0A000"
    );
    // The plain insert still works, so the refusal is about ON CONFLICT alone.
    assert!(tag(&mut s, "INSERT INTO s VALUES (1, 'a')").await == "INSERT 0 1");
}

// ---------------------------------------------------------------------------
// Extended protocol
// ---------------------------------------------------------------------------

fn text_param(value: &str) -> BoundParam {
    BoundParam {
        type_oid: None,
        format: 0,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
    }
}

/// `$n` placeholders bind inside `DO UPDATE SET` expressions and inside the
/// action `WHERE`, through the extended query protocol.
#[tokio::test]
async fn parameters_bind_in_do_update_set_and_action_where() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4 PRIMARY KEY, v text, n int4)",
        "INSERT INTO t VALUES (1, 'a', 10)",
    ])
    .await;

    s.parse(
        "upsert",
        "INSERT INTO t VALUES ($1, $2, $3) \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v || $4, n = t.n + $5 WHERE t.n < $6",
        &[],
    )
    .await
    .expect("parse parameterized upsert");

    // Conflicts on id=1 and the action WHERE is satisfied (10 < 100).
    s.bind(
        "p1",
        "upsert",
        &[
            text_param("1"),
            text_param("x"),
            text_param("0"),
            text_param("!"),
            text_param("5"),
            text_param("100"),
        ],
        &[],
    )
    .await
    .expect("bind updating row");
    let outcome = s.execute("p1", 0).await.expect("execute updating row");
    assert!(
        outcome
            == ExecuteOutcome::CommandComplete {
                tag: "INSERT 0 1".into()
            }
    );
    assert!(query(&mut s, "SELECT id, v, n FROM t").await == vec![row(&["1", "x!", "15"])]);

    // Same statement, a WHERE that is now false: nothing changes and nothing counts.
    s.bind(
        "p2",
        "upsert",
        &[
            text_param("1"),
            text_param("y"),
            text_param("0"),
            text_param("?"),
            text_param("5"),
            text_param("1"),
        ],
        &[],
    )
    .await
    .expect("bind non-updating row");
    let outcome = s.execute("p2", 0).await.expect("execute non-updating row");
    assert!(
        outcome
            == ExecuteOutcome::CommandComplete {
                tag: "INSERT 0 0".into()
            }
    );
    assert!(query(&mut s, "SELECT id, v, n FROM t").await == vec![row(&["1", "x!", "15"])]);

    // A non-conflicting key inserts the bound values.
    s.bind(
        "p3",
        "upsert",
        &[
            text_param("2"),
            text_param("b"),
            text_param("20"),
            text_param("!"),
            text_param("5"),
            text_param("100"),
        ],
        &[],
    )
    .await
    .expect("bind inserting row");
    let outcome = s.execute("p3", 0).await.expect("execute inserting row");
    assert!(
        outcome
            == ExecuteOutcome::CommandComplete {
                tag: "INSERT 0 1".into()
            }
    );
    assert!(
        query(&mut s, "SELECT id, v, n FROM t ORDER BY id").await
            == vec![row(&["1", "x!", "15"]), row(&["2", "b", "20"])]
    );
}
