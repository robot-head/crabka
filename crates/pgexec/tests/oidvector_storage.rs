//! A stored `oidvector` must come back an `oidvector`.
//!
//! The row encoding wrote `oidvector` under the plain array tag, so the decoder
//! rebuilt every one of them as `integer[]`. The column could still be
//! declared, inserted into and indexed — which is why nothing caught it — but
//! everything downstream of the read was wrong:
//!
//! * `SELECT` printed the array literal `[0:1]={1,2}` for a value written
//!   `1 2`, and that text is not input `oidvector` accepts, so a dump did not
//!   restore.
//! * An oid past 2^31 printed **signed**: `4294967295 0` came back
//!   `[0:1]={-1,0}`, because the array output prints an `Int4` element as an
//!   `Int4` while `oidvectorout` prints it as the unsigned oid it is.
//! * Every predicate against an `oidvector` literal was a hard 42804, `cannot
//!   compare integer[] and oidvector` — including the one behind an index scan.
//! * `UNION` and `DISTINCT` saw a stored value and the same value written as a
//!   literal as two different values, so neither folded them.
//! * A UNIQUE index over the column admitted a duplicate.
//!
//! `oidvector` survived this because the corpus only ever reaches it through
//! `pg_input_is_valid`/`pg_input_error_info`, which store nothing, and through
//! catalog columns the executor builds in memory. A user table is the one path
//! that writes one to disk, so that is what this covers.
//!
//! Every expectation here is `PostgreSQL` 18.4's.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row of a result, one string per row with the columns comma-joined.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    let results = session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
    match &results[0] {
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

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

/// The SQLSTATE a statement fails with.
async fn sqlstate(session: &mut SqlSession, sql: &str) -> String {
    match session.simple_query(sql).await {
        Ok(_) => panic!("{sql} should fail"),
        Err(error) => error.code,
    }
}

/// The text of a stored `oidvector`, its type, and the predicates over it.
#[tokio::test]
async fn a_stored_oidvector_reads_back_as_an_oidvector() {
    let mut session = SqlEngine::new().connect();
    run(&mut session, "CREATE TABLE ov (id int, v oidvector)").await;
    run(&mut session, "INSERT INTO ov VALUES (1, '1 2')").await;
    // An oid past 2^31 is the case the array reading corrupted rather than
    // merely misspelled.
    run(&mut session, "INSERT INTO ov VALUES (2, '4294967295 0')").await;

    assert!(
        query(&mut session, "SELECT v FROM ov ORDER BY id").await
            == ["1 2", "4294967295 0"].map(String::from)
    );
    assert!(
        query(&mut session, "SELECT pg_typeof(v) FROM ov ORDER BY id").await
            == ["oidvector", "oidvector"].map(String::from)
    );
    // The rendered text is the space-separated form `oidvector` reads back,
    // which is what a dump needs; the array literal it used to print is not.
    assert!(
        query(&mut session, "SELECT v::text FROM ov ORDER BY id").await
            == ["1 2", "4294967295 0"].map(String::from)
    );
    // The comparison that was 42804 for every stored row.
    assert!(
        query(
            &mut session,
            "SELECT v = '1 2'::oidvector FROM ov ORDER BY id"
        )
        .await
            == ["t", "f"].map(String::from)
    );
    assert!(
        query(&mut session, "SELECT v FROM ov WHERE v = '1 2'::oidvector").await
            == ["1 2"].map(String::from)
    );
    // A stored value and the same value as a literal are ONE value, so the set
    // operators fold them.
    assert!(
        query(
            &mut session,
            "SELECT v FROM ov WHERE id = 1 UNION SELECT '1 2'::oidvector"
        )
        .await
            == ["1 2"].map(String::from)
    );
}

/// An index over an `oidvector` column: the probe finds the row, and the unique
/// constraint sees the duplicate it used to let through.
#[tokio::test]
async fn an_index_over_an_oidvector_column_probes_and_enforces() {
    let mut session = SqlEngine::new().connect();
    run(&mut session, "CREATE TABLE ov (id int, v oidvector)").await;
    run(&mut session, "CREATE UNIQUE INDEX ov_v ON ov (v)").await;
    run(&mut session, "INSERT INTO ov VALUES (1, '1 2')").await;

    assert!(
        query(&mut session, "SELECT id FROM ov WHERE v = '1 2'::oidvector").await
            == ["1"].map(String::from)
    );
    assert!(sqlstate(&mut session, "INSERT INTO ov VALUES (2, '1 2')").await == "23505");
    assert!(query(&mut session, "SELECT count(*) FROM ov").await == ["1"].map(String::from));
}

/// A stored `oidvector` copied into another table keeps its type, and joins on
/// it match: both sides are vectors now, where a stored one used to be an
/// array and a literal one a vector.
#[tokio::test]
async fn an_oidvector_survives_a_copy_between_tables() {
    let mut session = SqlEngine::new().connect();
    run(&mut session, "CREATE TABLE ov (id int, v oidvector)").await;
    run(&mut session, "INSERT INTO ov VALUES (1, '1 2')").await;
    run(&mut session, "CREATE TABLE ov2 (id int, v oidvector)").await;
    run(&mut session, "INSERT INTO ov2 SELECT id, v FROM ov").await;

    assert!(query(&mut session, "SELECT v FROM ov2").await == ["1 2"].map(String::from));
    assert!(
        query(&mut session, "SELECT count(*) FROM ov JOIN ov2 USING (v)").await
            == ["1"].map(String::from)
    );
}

/// Ordering is unsigned, because an oid is.
///
/// The elements ride in `Int4` with their bit pattern intact, so the generic
/// element-wise comparison read them back signed and sorted `4294967295` --
/// the largest oid there is -- below `1`. `PostgreSQL` compares through
/// `oidcmp`. This became reachable only once a stored vector decoded as an
/// `oidvector` at all; before that every one of these predicates was a hard
/// 42804 and the ordering never ran.
///
/// `int2vector` shares the same datum variant and its elements really are
/// signed, so the two must disagree -- that pair is the whole reason the
/// unsigned reading keys off the element type rather than the variant.
#[tokio::test]
async fn a_stored_oidvector_orders_unsigned() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE ov (id int4, v oidvector)").await;
    run(
        &mut session,
        "INSERT INTO ov VALUES (1, '1 2'), (2, '4294967295 0')",
    )
    .await;

    assert!(query(&mut session, "SELECT v FROM ov ORDER BY v").await == ["1 2", "4294967295 0"]);
    assert!(
        query(&mut session, "SELECT v FROM ov ORDER BY v DESC").await == ["4294967295 0", "1 2"]
    );

    // Both rows exceed '1 1': 4294967295 is above it unsigned, and would be
    // below it read as -1.
    let above = query(
        &mut session,
        "SELECT id FROM ov WHERE v > '1 1'::oidvector ORDER BY id",
    )
    .await;
    assert!(above == ["1", "2"]);
    let below = query(&mut session, "SELECT id FROM ov WHERE v < '1 1'::oidvector").await;
    assert!(below.is_empty());

    // min and max read the same ordering.
    assert!(query(&mut session, "SELECT max(v) FROM ov").await == ["4294967295 0"]);
    assert!(query(&mut session, "SELECT min(v) FROM ov").await == ["1 2"]);
}
