//! The `DETAIL` line that follows a partition error, and who is allowed to see
//! it.
//!
//! `PostgreSQL` reports two different descriptions after two different
//! failures, and the difference matters to anyone reading the message.
//!
//! * A row that no partition accepts reports **the partition key alone**,
//!   spelled `(key) = (values)`. The relation named is the one whose own
//!   partition list declined the row, which in a multi-level tree is not the
//!   relation the statement named.
//! * A row written straight into a leaf that falls outside a bound reports
//!   **the whole row**, every column in that leaf's attribute order.
//!
//! Both descriptions quote the contents of a row, so both are gated: upstream
//! builds them in `ExecBuildSlotPartitionKeyDescription` and
//! `ExecBuildSlotValueDescription`, and both answer with nothing at all when
//! the caller holds no `SELECT` on the relation or a row-level security policy
//! is active on it. Those two cases are asserted here as carefully as the
//! wording is, because a message that leaks a row is worse than a message that
//! is merely wrong.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

/// The three fields of a failure this file judges: `SQLSTATE`, primary message,
/// and `DETAIL`. Compared whole rather than field by field, so a case that
/// gains an unexpected `DETAIL` fails instead of passing quietly.
#[derive(Debug, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
    detail: Option<String>,
}

impl Failure {
    fn new(code: &str, message: &str, detail: Option<&str>) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            detail: detail.map(ToString::to_string),
        }
    }
}

async fn failure(session: &mut SqlSession, sql: &str) -> Failure {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    Failure {
        code: error.code.clone(),
        message: error.message.clone(),
        detail: error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.clone()),
    }
}

async fn engine_with(setup: &str) -> SqlEngine {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    let mut session = engine.connect();
    run(&mut session, setup).await;
    engine
}

/// A routing failure: the setup, the `INSERT` that finds no partition, the
/// relation the error must name, and the key description that must follow it.
struct RoutingCase {
    what: &'static str,
    setup: &'static str,
    insert: &'static str,
    relation: &'static str,
    key: &'static str,
}

/// Every partition strategy reports the key of the row it could not place.
///
/// The value is rendered by its type's *output* function, not as a literal, so
/// a string carries no quotes; a NULL key prints as the bare word `null`; and a
/// multi-column key prints its columns and its values as two matching lists.
#[tokio::test]
async fn a_routing_failure_reports_the_partition_key_of_the_row() {
    // The hash case uses `2`, which `MODULUS 4` places in neither of the two
    // remainders declared, whereas `1` and `3` both land in one of them.
    let cases = [
        RoutingCase {
            what: "range",
            setup: "CREATE TABLE r (a int, b text) PARTITION BY RANGE (a);
                    CREATE TABLE r0 PARTITION OF r FOR VALUES FROM (0) TO (10)",
            insert: "INSERT INTO r VALUES (10, 'x')",
            relation: "r",
            key: "(a) = (10)",
        },
        RoutingCase {
            what: "list",
            setup: "CREATE TABLE l (a text, b int) PARTITION BY LIST (a);
                    CREATE TABLE l0 PARTITION OF l FOR VALUES IN ('keep')",
            insert: "INSERT INTO l VALUES ('drop', 1)",
            relation: "l",
            key: "(a) = (drop)",
        },
        RoutingCase {
            what: "hash",
            setup: "CREATE TABLE h (a int) PARTITION BY HASH (a);
                    CREATE TABLE h0 PARTITION OF h FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                    CREATE TABLE h1 PARTITION OF h FOR VALUES WITH (MODULUS 4, REMAINDER 1)",
            insert: "INSERT INTO h VALUES (2)",
            relation: "h",
            key: "(a) = (2)",
        },
        RoutingCase {
            what: "a multi-column key",
            setup: "CREATE TABLE m (a int, b int, c text) PARTITION BY RANGE (a, b);
                    CREATE TABLE m0 PARTITION OF m FOR VALUES FROM (0, 0) TO (1, 10)",
            insert: "INSERT INTO m VALUES (1, 20, 'x')",
            relation: "m",
            key: "(a, b) = (1, 20)",
        },
        RoutingCase {
            what: "a NULL in the key",
            setup: "CREATE TABLE n (a int, b text) PARTITION BY LIST (b);
                    CREATE TABLE n0 PARTITION OF n FOR VALUES IN ('a')",
            insert: "INSERT INTO n (a) VALUES (1)",
            relation: "n",
            key: "(b) = (null)",
        },
        RoutingCase {
            what: "a NULL in one column of a multi-column key",
            setup: "CREATE TABLE mn (a int, b int) PARTITION BY RANGE (a, b);
                    CREATE TABLE mn0 PARTITION OF mn FOR VALUES FROM (0, 0) TO (1, 10)",
            insert: "INSERT INTO mn (a) VALUES (5)",
            relation: "mn",
            key: "(a, b) = (5, null)",
        },
        RoutingCase {
            what: "a key column needing quotes",
            setup: "CREATE TABLE q (\"Key\" int) PARTITION BY RANGE (\"Key\");
                    CREATE TABLE q0 PARTITION OF q FOR VALUES FROM (0) TO (10)",
            insert: "INSERT INTO q VALUES (10)",
            relation: "q",
            key: "(\"Key\") = (10)",
        },
    ];
    for case in cases {
        let engine = engine_with(case.setup).await;
        let mut session = engine.connect();
        assert!(
            failure(&mut session, case.insert).await
                == Failure::new(
                    "23514",
                    &format!(
                        "no partition of relation \"{}\" found for row",
                        case.relation
                    ),
                    Some(&format!(
                        "Partition key of the failing row contains {}.",
                        case.key
                    )),
                ),
            "{}",
            case.what
        );
    }
}

/// A bound violation: the setup, the `INSERT` straight into a leaf, the leaf
/// named, and the whole-row description that must follow.
struct BoundCase {
    what: &'static str,
    setup: &'static str,
    insert: &'static str,
    relation: &'static str,
    row: &'static str,
}

/// A row written straight into a leaf reports the *whole* row, not the key.
///
/// Every column in the leaf's own attribute order, including the ones the
/// partition key never looks at, and a NULL in any of them as the bare word
/// `null`.
#[tokio::test]
async fn a_bound_violation_reports_the_whole_failing_row() {
    let cases = [
        BoundCase {
            what: "range",
            setup: "CREATE TABLE r (a int, b text) PARTITION BY RANGE (a);
                    CREATE TABLE r0 PARTITION OF r FOR VALUES FROM (0) TO (10)",
            insert: "INSERT INTO r0 VALUES (10, 'abc')",
            relation: "r0",
            row: "(10, abc)",
        },
        BoundCase {
            what: "list",
            setup: "CREATE TABLE l (a text, b int) PARTITION BY LIST (a);
                    CREATE TABLE l0 PARTITION OF l FOR VALUES IN ('keep')",
            insert: "INSERT INTO l0 VALUES ('drop', 7)",
            relation: "l0",
            row: "(drop, 7)",
        },
        BoundCase {
            what: "hash",
            setup: "CREATE TABLE h (a int) PARTITION BY HASH (a);
                    CREATE TABLE h0 PARTITION OF h FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                    CREATE TABLE h1 PARTITION OF h FOR VALUES WITH (MODULUS 4, REMAINDER 1)",
            insert: "INSERT INTO h0 VALUES (2)",
            relation: "h0",
            row: "(2)",
        },
        BoundCase {
            what: "a NULL in a column the key never reads",
            setup: "CREATE TABLE nk (a int, b text) PARTITION BY RANGE (a);
                    CREATE TABLE nk0 PARTITION OF nk FOR VALUES FROM (0) TO (10)",
            insert: "INSERT INTO nk0 (a) VALUES (10)",
            relation: "nk0",
            row: "(10, null)",
        },
        BoundCase {
            what: "a NULL in the key column",
            setup: "CREATE TABLE nn (a int, b text) PARTITION BY RANGE (a);
                    CREATE TABLE nn0 PARTITION OF nn FOR VALUES FROM (0) TO (10)",
            insert: "INSERT INTO nn0 (b) VALUES ('x')",
            relation: "nn0",
            row: "(null, x)",
        },
        BoundCase {
            what: "a multi-column key",
            setup: "CREATE TABLE m (a int, b int, c text) PARTITION BY RANGE (a, b);
                    CREATE TABLE m0 PARTITION OF m FOR VALUES FROM (0, 0) TO (1, 10)",
            insert: "INSERT INTO m0 VALUES (1, 20, 'x')",
            relation: "m0",
            row: "(1, 20, x)",
        },
    ];
    for case in cases {
        let engine = engine_with(case.setup).await;
        let mut session = engine.connect();
        assert!(
            failure(&mut session, case.insert).await
                == Failure::new(
                    "23514",
                    &format!(
                        "new row for relation \"{}\" violates partition constraint",
                        case.relation
                    ),
                    Some(&format!("Failing row contains {}.", case.row)),
                ),
            "{}",
            case.what
        );
    }
}

/// The relation named is the level that declined the row, not the level the
/// statement named.
///
/// A row entering `ml` is placed in `ml1` by `ml`'s key and only then judged by
/// `ml1`'s. When `ml1` has nowhere to put it, the row failed against `ml1`'s
/// key, and naming `ml` would print a key the row routed through successfully —
/// so the reported key here is `(b)`, which is not a partition key of `ml` at
/// all.
#[tokio::test]
async fn a_multi_level_tree_reports_the_level_that_declined_the_row() {
    let engine = engine_with(
        "CREATE TABLE ml (a int, b int) PARTITION BY LIST (a);
         CREATE TABLE ml1 PARTITION OF ml FOR VALUES IN (1) PARTITION BY RANGE (b);
         CREATE TABLE ml1a PARTITION OF ml1 FOR VALUES FROM (0) TO (10)",
    )
    .await;
    let mut session = engine.connect();
    // Declined by the root: `a` is the key it failed against.
    assert!(
        failure(&mut session, "INSERT INTO ml VALUES (2, 5)").await
            == Failure::new(
                "23514",
                "no partition of relation \"ml\" found for row",
                Some("Partition key of the failing row contains (a) = (2)."),
            )
    );
    // Routed through the root, declined by the middle level: `b` is the key,
    // and `ml1` is the relation.
    assert!(
        failure(&mut session, "INSERT INTO ml VALUES (1, 50)").await
            == Failure::new(
                "23514",
                "no partition of relation \"ml1\" found for row",
                Some("Partition key of the failing row contains (b) = (50)."),
            )
    );
}

/// A leaf's own bound is not the whole constraint: every ancestor's bound
/// applies to a row written straight into it.
///
/// `ml1a` accepts `b` in `[0, 10)` and its grandparent `ml` accepts only
/// `a = 1`. A row with `a = 99` satisfies the leaf and breaks the root, and
/// storing it would put a row in `ml`'s tree that no `INSERT INTO ml` could
/// ever have routed there and that `SELECT … FROM ml` would then hand back.
#[tokio::test]
async fn an_ancestor_bound_refuses_a_row_the_leafs_own_bound_admits() {
    let engine = engine_with(
        "CREATE TABLE ml (a int, b int) PARTITION BY LIST (a);
         CREATE TABLE ml1 PARTITION OF ml FOR VALUES IN (1) PARTITION BY RANGE (b);
         CREATE TABLE ml1a PARTITION OF ml1 FOR VALUES FROM (0) TO (10)",
    )
    .await;
    let mut session = engine.connect();
    assert!(
        failure(&mut session, "INSERT INTO ml1a VALUES (99, 5)").await
            == Failure::new(
                "23514",
                "new row for relation \"ml1a\" violates partition constraint",
                Some("Failing row contains (99, 5)."),
            )
    );
    // The leaf's own bound still refuses on its own account, and a row inside
    // both bounds is still accepted.
    assert!(
        failure(&mut session, "INSERT INTO ml1a VALUES (1, 50)").await
            == Failure::new(
                "23514",
                "new row for relation \"ml1a\" violates partition constraint",
                Some("Failing row contains (1, 50)."),
            )
    );
    run(&mut session, "INSERT INTO ml1a VALUES (1, 5)").await;
}

/// The same rule reached the other way: a write naming a *middle* level is
/// routed downwards from there, and the level it was named at still has to
/// admit the row.
///
/// This is `insert.out`'s `part_default` shape. `ml1` routes the row to `ml1a`
/// happily — `b` is in range — and only `ml`'s bound, two levels above the
/// leaf, refuses it. A check that stopped at the leaf's own parent would store
/// it.
#[tokio::test]
async fn a_write_naming_a_middle_level_is_still_judged_by_the_levels_above_it() {
    let engine = engine_with(
        "CREATE TABLE ml (a int, b int) PARTITION BY LIST (a);
         CREATE TABLE ml1 PARTITION OF ml FOR VALUES IN (1) PARTITION BY RANGE (b);
         CREATE TABLE ml1a PARTITION OF ml1 FOR VALUES FROM (0) TO (10)",
    )
    .await;
    let mut session = engine.connect();
    assert!(
        failure(&mut session, "INSERT INTO ml1 VALUES (99, 5)").await
            == Failure::new(
                "23514",
                "new row for relation \"ml1a\" violates partition constraint",
                Some("Failing row contains (99, 5)."),
            )
    );
    run(&mut session, "INSERT INTO ml1 VALUES (1, 5)").await;
    assert!(rows_of(&mut session, "SELECT a, b FROM ml").await == vec!["1,5".to_string()]);
}

/// The rows a relation holds, as comma-joined text, for the assertions that
/// have to show a refusal stored nothing.
async fn rows_of(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match run(session, sql).await.pop().expect("one result") {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        cell.as_ref().map_or_else(
                            || "NULL".to_string(),
                            |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// A caller who may not read the relation is told only what it already knows.
///
/// The two descriptions are gated differently, and the difference is upstream's:
///
/// * The **key** description has no middle form. Without `SELECT` the caller is
///   told nothing, because the key columns are not necessarily columns it
///   supplied.
/// * The **row** description falls back to the columns the statement wrote.
///   Echoing a value the caller passed in is no disclosure — it already had it —
///   so upstream keeps those and drops the rest, naming what it kept. A column
///   the caller never named is the one that would leak, and that is the one
///   withheld.
#[tokio::test]
async fn a_caller_without_select_is_told_only_the_columns_it_supplied() {
    let engine = engine_with(
        "CREATE TABLE p (a int, b text, c text DEFAULT 'classified') PARTITION BY RANGE (a);
         CREATE TABLE p0 PARTITION OF p FOR VALUES FROM (0) TO (10);
         CREATE ROLE writer;
         GRANT INSERT ON p TO writer;
         GRANT INSERT ON p0 TO writer",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "SET ROLE writer").await;
    let routing = "INSERT INTO p VALUES (10, 'secret', 'secret')";
    let bound = "INSERT INTO p0 VALUES (10, 'secret', 'secret')";
    // No key description at all: the caller cannot read the relation.
    assert!(
        failure(&mut session, routing).await
            == Failure::new(
                "23514",
                "no partition of relation \"p\" found for row",
                None
            )
    );
    // Every column was supplied, so every column may be echoed — but the form
    // names its columns, which is how a reader tells it from the whole row.
    assert!(
        failure(&mut session, bound).await
            == Failure::new(
                "23514",
                "new row for relation \"p0\" violates partition constraint",
                Some("Failing row contains (a, b, c) = (10, secret, secret)."),
            )
    );
    // `c` took its default, so the caller never supplied it and is not shown
    // it — this is the column the ungated message would have leaked.
    assert!(
        failure(&mut session, "INSERT INTO p0 (a, b) VALUES (10, 'secret')").await
            == Failure::new(
                "23514",
                "new row for relation \"p0\" violates partition constraint",
                Some("Failing row contains (a, b) = (10, secret)."),
            )
    );
    // Nothing supplied that the caller may see means no description at all.
    assert!(
        failure(&mut session, "INSERT INTO p0 (a) VALUES (10)").await
            == Failure::new(
                "23514",
                "new row for relation \"p0\" violates partition constraint",
                Some("Failing row contains (a) = (10)."),
            )
    );
    // The same two statements, once the caller may read the relations: the key
    // description appears, and the row description drops its column list
    // because every column is now readable rather than merely supplied.
    let mut owner = engine.connect();
    run(
        &mut owner,
        "GRANT SELECT ON p TO writer; GRANT SELECT ON p0 TO writer",
    )
    .await;
    assert!(
        failure(&mut session, routing).await
            == Failure::new(
                "23514",
                "no partition of relation \"p\" found for row",
                Some("Partition key of the failing row contains (a) = (10)."),
            )
    );
    assert!(
        failure(&mut session, bound).await
            == Failure::new(
                "23514",
                "new row for relation \"p0\" violates partition constraint",
                Some("Failing row contains (10, secret, secret)."),
            )
    );
    assert!(
        failure(&mut session, "INSERT INTO p0 (a) VALUES (10)").await
            == Failure::new(
                "23514",
                "new row for relation \"p0\" violates partition constraint",
                Some("Failing row contains (10, null, classified)."),
            )
    );
}

/// The same gate, and the same fallback, on a `WITH CHECK OPTION` violation.
///
/// This is upstream's `WCO_VIEW_CHECK`, which builds its `DETAIL` with the same
/// `ExecBuildSlotValueDescription` the partition errors use. The leak it closes
/// is the sharper one: an `UPDATE` through a view needs no `SELECT` on the base
/// relation, so before the gate the message handed back the *stored* values of
/// columns the statement never touched and the caller could not have read.
#[tokio::test]
async fn a_check_option_violation_withholds_the_columns_the_caller_cannot_read() {
    let engine = engine_with(
        "CREATE TABLE base (a int, secret text);
         INSERT INTO base VALUES (1, 'classified');
         CREATE VIEW v AS SELECT a, secret FROM base WHERE a < 10 WITH LOCAL CHECK OPTION;
         CREATE ROLE writer;
         GRANT UPDATE ON v TO writer;
         GRANT UPDATE ON base TO writer",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "SET ROLE writer").await;
    // `secret` was never assigned and may not be read, so it is withheld; `a`
    // is echoed because the caller supplied it.
    assert!(
        failure(&mut session, "UPDATE v SET a = 30").await
            == Failure::new(
                "44000",
                "new row violates check option for view \"v\"",
                Some("Failing row contains (a) = (30)."),
            )
    );
    // With SELECT the whole row appears, stored column and all.
    let mut owner = engine.connect();
    run(
        &mut owner,
        "GRANT SELECT ON v TO writer; GRANT SELECT ON base TO writer",
    )
    .await;
    assert!(
        failure(&mut session, "UPDATE v SET a = 30").await
            == Failure::new(
                "44000",
                "new row violates check option for view \"v\"",
                Some("Failing row contains (30, classified)."),
            )
    );
}

/// Neither description is shown where a row-level security policy is active.
///
/// A policy exists precisely to stop this caller from seeing rows it did not
/// write, and a description built from a rejected row would route around it.
/// The caller here holds `SELECT` outright, so only the policy withholds the
/// description — and dropping the policy restores it.
#[tokio::test]
async fn an_active_row_security_policy_withholds_both_descriptions() {
    let engine = engine_with(
        "CREATE TABLE s (a int, b text) PARTITION BY RANGE (a);
         CREATE TABLE s0 PARTITION OF s FOR VALUES FROM (0) TO (10);
         CREATE ROLE writer;
         GRANT INSERT, SELECT ON s TO writer;
         GRANT INSERT, SELECT ON s0 TO writer;
         ALTER TABLE s ENABLE ROW LEVEL SECURITY;
         ALTER TABLE s0 ENABLE ROW LEVEL SECURITY;
         CREATE POLICY open_s ON s USING (true) WITH CHECK (true);
         CREATE POLICY open_s0 ON s0 USING (true) WITH CHECK (true)",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "SET ROLE writer").await;
    let routing = "INSERT INTO s VALUES (10, 'secret')";
    let bound = "INSERT INTO s0 VALUES (10, 'secret')";
    assert!(
        failure(&mut session, routing).await
            == Failure::new(
                "23514",
                "no partition of relation \"s\" found for row",
                None
            )
    );
    assert!(
        failure(&mut session, bound).await
            == Failure::new(
                "23514",
                "new row for relation \"s0\" violates partition constraint",
                None,
            )
    );
    let mut owner = engine.connect();
    run(
        &mut owner,
        "ALTER TABLE s DISABLE ROW LEVEL SECURITY; ALTER TABLE s0 DISABLE ROW LEVEL SECURITY",
    )
    .await;
    assert!(
        failure(&mut session, routing).await
            == Failure::new(
                "23514",
                "no partition of relation \"s\" found for row",
                Some("Partition key of the failing row contains (a) = (10)."),
            )
    );
    assert!(
        failure(&mut session, bound).await
            == Failure::new(
                "23514",
                "new row for relation \"s0\" violates partition constraint",
                Some("Failing row contains (10, secret)."),
            )
    );
}

/// A value longer than `PostgreSQL`'s 64-*byte* field budget is cut, and cut on
/// a character boundary.
///
/// The budget is bytes, not characters, which only a multi-byte value can tell
/// apart — and the two multi-byte cases below are the two that differ. A
/// 2-byte character divides 64 exactly, so the cut lands on a boundary
/// unaided; a 3-byte character does not, so the cut must retreat from 64 to 63
/// rather than split the character. That retreat is `pg_mbcliplen`, and a
/// character-counting cut gets both the length and the boundary wrong.
///
/// The `...` is upstream's, appended after the cut rather than counted inside
/// it.
#[tokio::test]
async fn a_long_value_is_cut_to_the_field_budget_on_a_character_boundary() {
    let engine = engine_with(
        "CREATE TABLE t (a int, b text) PARTITION BY RANGE (a);
         CREATE TABLE t0 PARTITION OF t FOR VALUES FROM (0) TO (10)",
    )
    .await;
    let mut session = engine.connect();
    // Each case: a character, how many of it to write, and how many survive the
    // cut. 64 bytes of `x`, 32 of `é` (64 bytes), 21 of `€` (63 bytes, the
    // most that fit without splitting the 22nd).
    let cases = [("x", 70, 64), ("é", 40, 32), ("€", 30, 21)];
    for (character, written, kept) in cases {
        let value = character.repeat(written);
        assert!(
            failure(
                &mut session,
                &format!("INSERT INTO t0 VALUES (10, '{value}')")
            )
            .await
            .detail
                == Some(format!(
                    "Failing row contains (10, {}...).",
                    character.repeat(kept)
                )),
            "{written} x {character:?}"
        );
    }
    // A value exactly at the budget is not cut, and carries no `...`.
    let exact = "x".repeat(64);
    assert!(
        failure(
            &mut session,
            &format!("INSERT INTO t0 VALUES (10, '{exact}')")
        )
        .await
        .detail
            == Some(format!("Failing row contains (10, {exact})."))
    );
}
