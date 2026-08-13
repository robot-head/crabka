//! A partition key survives `DROP COLUMN` on the relation it is keyed on.
//!
//! `DROP COLUMN` compacts Crabka's column list and every stored row, unlike
//! `PostgreSQL`, which leaves the attribute in place and sets `attisdropped`.
//! A partition key that recorded a column *position* therefore pointed at the
//! neighbouring column as soon as anything before it was dropped, and a
//! partitioned table then routed rows into the wrong leaf with no error at all.
//! The key records the column's name instead, and the position is resolved
//! against the live column list at every use.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, Session};

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code, error.message)
}

/// Every row of `sql`, each rendered as its comma-joined text columns.
async fn rows_of(session: &mut SqlSession, sql: &str) -> Vec<String> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
        .iter()
        .filter_map(|outcome| match outcome {
            crabka_pgwire::engine::QueryResult::Rows { rows, .. } => Some(rows),
            _ => None,
        })
        .flatten()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_ref().map_or_else(
                        || "NULL".to_string(),
                        |cell| String::from_utf8_lossy(&cell.text).into_owned(),
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

/// The defect in its plainest form. `a` is declared second, so a key recorded
/// as position 1 read `a` before the drop and `b` after it -- and both writes
/// below then landed in the partition that names the *other* value.
#[tokio::test]
async fn a_row_routes_by_its_key_column_after_an_earlier_column_is_dropped() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE p (fdrop int, a int, b int) PARTITION BY LIST (a)",
    )
    .await;
    run(&mut session, "ALTER TABLE p DROP COLUMN fdrop").await;
    run(
        &mut session,
        "CREATE TABLE p1 PARTITION OF p FOR VALUES IN (1)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE p2 PARTITION OF p FOR VALUES IN (2)",
    )
    .await;
    run(&mut session, "INSERT INTO p VALUES (1, 2), (2, 1)").await;

    assert!(rows_of(&mut session, "SELECT a, b FROM p1").await == vec!["1,2"]);
    assert!(rows_of(&mut session, "SELECT a, b FROM p2").await == vec!["2,1"]);
    // A read through the parent unions every leaf and filters, so it answered
    // correctly even while the leaves held each other's rows -- which is what
    // made the misplacement quiet. `tableoid` is the read that does not.
    assert!(
        rows_of(
            &mut session,
            "SELECT tableoid::regclass, a FROM p ORDER BY a"
        )
        .await
            == vec!["p1,1", "p2,2"]
    );
}

/// The same slide, seen through `pg_partitioned_table`. `partattrs` is an
/// attribute number, and Crabka's attribute numbers are positions in the live
/// column list, so the drop has to move it.
#[tokio::test]
async fn partattrs_reports_the_key_column_position_the_relation_has_now() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE p (fdrop int, a int, b int) PARTITION BY LIST (a)",
    )
    .await;
    let attrs = "SELECT partattrs FROM pg_partitioned_table WHERE partrelid = 'p'::regclass";
    assert!(rows_of(&mut session, attrs).await == vec!["2"]);

    run(&mut session, "ALTER TABLE p DROP COLUMN fdrop").await;
    assert!(rows_of(&mut session, attrs).await == vec!["1"]);
    // The key still prints under the name it was written with.
    assert!(
        rows_of(&mut session, "SELECT pg_get_partkeydef('p'::regclass)").await == vec!["LIST (a)"]
    );
}

/// `foreign_key.sql`'s shape: both the parent and the candidate partition lose
/// columns before they are joined, and the columns they lose are at different
/// positions on the two sides.
#[tokio::test]
async fn a_partition_attaches_after_both_sides_have_dropped_columns() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE fk_partitioned_fk (b int, fdrop1 int, a int) PARTITION BY RANGE (a, b)",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE fk_partitioned_fk DROP COLUMN fdrop1",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE fk_partitioned_fk_1 (fdrop1 int, fdrop2 int, a int, fdrop3 int, b int)",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE fk_partitioned_fk_1 DROP COLUMN fdrop1, DROP COLUMN fdrop2, DROP COLUMN \
         fdrop3",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_1 FOR VALUES FROM (0,0) \
         TO (1000,1000)",
    )
    .await;

    run(
        &mut session,
        "INSERT INTO fk_partitioned_fk (a, b) VALUES (500, 501)",
    )
    .await;
    assert!(rows_of(&mut session, "SELECT a, b FROM fk_partitioned_fk_1").await == vec!["500,501"]);
    // The parent declares `(b, a)`; the leaf declares `(a, b)`. Reading through
    // the parent has to present the parent's order.
    assert!(rows_of(&mut session, "SELECT * FROM fk_partitioned_fk").await == vec!["501,500"]);
    assert!(
        rows_of(
            &mut session,
            "SELECT a, b FROM fk_partitioned_fk WHERE a = 500"
        )
        .await
            == vec!["500,501"]
    );
}

/// A key column cannot be dropped at all, which is what keeps every stored key
/// name resolvable. `PostgreSQL` refuses this for its own reason -- the key's
/// dependency would cascade the whole table away -- with the same 42P16.
#[tokio::test]
async fn dropping_a_partition_key_column_is_refused() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE partitioned (a int, b int, c int) PARTITION BY RANGE (a, b)",
    )
    .await;

    for column in ["a", "b"] {
        assert!(
            error_of(
                &mut session,
                &format!("ALTER TABLE partitioned DROP COLUMN {column}")
            )
            .await
                == (
                    "42P16".to_string(),
                    format!(
                        "cannot drop column \"{column}\" because it is part of the partition key \
                         of relation \"partitioned\""
                    )
                )
        );
    }
    // A column the key does not name still goes, and the key still works.
    run(&mut session, "ALTER TABLE partitioned DROP COLUMN c").await;
    run(
        &mut session,
        "CREATE TABLE partitioned_1 PARTITION OF partitioned FOR VALUES FROM (0,0) TO (10,10)",
    )
    .await;
    run(&mut session, "INSERT INTO partitioned VALUES (1, 2)").await;
    assert!(rows_of(&mut session, "SELECT a, b FROM partitioned_1").await == vec!["1,2"]);
}

/// The refusal is per relation, so a sub-partitioned descendant stops a drop
/// that the relation the statement named would have allowed. `ATExecDropColumn`
/// runs its own check on each level of the recursion for the same reason.
#[tokio::test]
async fn a_descendants_own_key_refuses_a_drop_that_recursed_into_it() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE top (a int, b int, c int) PARTITION BY LIST (a)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE mid PARTITION OF top FOR VALUES IN (1) PARTITION BY LIST (b)",
    )
    .await;

    assert!(
        error_of(&mut session, "ALTER TABLE top DROP COLUMN b").await
            == (
                "42P16".to_string(),
                "cannot drop column \"b\" because it is part of the partition key of relation \
                 \"mid\""
                    .to_string()
            )
    );
    // Nothing was written: `mid` still has `b`, and `top` still has all three.
    assert!(
        rows_of(&mut session, "SELECT pg_get_partkeydef('mid'::regclass)").await
            == vec!["LIST (b)"]
    );
    assert!(
        rows_of(
            &mut session,
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'top'"
        )
        .await
            == vec!["3"]
    );
}

/// The other half of `has_partition_attrs`. A retype leaves every stored bound
/// coerced to the type the key column no longer has, and the relation then
/// reports `corrupt storage` on the next write it cannot compare. `PostgreSQL`
/// refuses the retype instead, with the same 42P16.
#[tokio::test]
async fn retyping_a_partition_key_column_is_refused() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (a int, b int) PARTITION BY RANGE (a)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE t1 PARTITION OF t FOR VALUES FROM (0) TO (100)",
    )
    .await;

    assert!(
        error_of(&mut session, "ALTER TABLE t ALTER COLUMN a TYPE text").await
            == (
                "42P16".to_string(),
                "cannot alter column \"a\" because it is part of the partition key of relation \
                 \"t\""
                    .to_string()
            )
    );
    // The refusal left nothing half-changed: the key still routes.
    run(&mut session, "INSERT INTO t VALUES (7, 2)").await;
    assert!(rows_of(&mut session, "SELECT a, b FROM t1").await == vec!["7,2"]);
    // A column outside the key still retypes.
    run(&mut session, "ALTER TABLE t ALTER COLUMN b TYPE text").await;
}

/// `RENAME COLUMN` and `DROP COLUMN` compose. The rename rewrites the key, and
/// the drop then slides the renamed column without the key losing track of it.
#[tokio::test]
async fn a_renamed_key_column_still_routes_after_a_later_drop() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE r (fdrop int, a int, b int) PARTITION BY LIST (a)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE r1 PARTITION OF r FOR VALUES IN (1)",
    )
    .await;
    run(&mut session, "ALTER TABLE r RENAME COLUMN a TO z").await;
    run(&mut session, "ALTER TABLE r DROP COLUMN fdrop").await;

    assert!(
        rows_of(&mut session, "SELECT pg_get_partkeydef('r'::regclass)").await == vec!["LIST (z)"]
    );
    run(&mut session, "INSERT INTO r VALUES (1, 42)").await;
    assert!(rows_of(&mut session, "SELECT z, b FROM r1").await == vec!["1,42"]);
    // The rename made `z` a key column; the drop guard follows the new name.
    assert!(
        error_of(&mut session, "ALTER TABLE r DROP COLUMN z")
            .await
            .0
            == "42P16"
    );
}
