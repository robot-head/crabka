//! A bit-string column DEFAULT keeps the type of the literal that was written,
//! not the type of the column it was written into.
//!
//! `PostgreSQL` stores a default as an expression with the coercion wrapped
//! around the literal, and `pg_get_expr` hides a binary-coercible cast, so
//! `B'0101'` in a `bit varying(5)` column deparses `'0101'::"bit"` — the type
//! of the literal — while a bare `'1001'` in the same column deparses
//! `'1001'::bit varying`. crabka stores the value rather than the expression,
//! so the datum's own `varying` flag is the only surviving record of which was
//! written, and it has to survive the coercion into the column.
//!
//! `bit_defaults` and every expected string here are `PostgreSQL` 18.4's, from
//! `src/test/regress/expected/bit.out`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"))
}

async fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &run(s, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// `bit.out`'s own table: two `bit(4)` columns and two `bit varying(5)`, each
/// pair written once as a bare string literal and once as a `B'...'` literal.
async fn bit_defaults_session() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE bit_defaults(
             b1 bit(4) DEFAULT '1001',
             b2 bit(4) DEFAULT B'0101',
             b3 bit varying(5) DEFAULT '1001',
             b4 bit varying(5) DEFAULT B'0101')",
    )
    .await;
    (engine, s)
}

/// The deparse. All four rows together, so the two spellings are pinned apart
/// rather than one of them being pinned alone — `b3` and `b4` differ only in
/// the `B` prefix, and before this they read the same.
#[tokio::test]
async fn a_bit_string_default_deparses_with_the_written_literals_type() {
    let (_engine, mut s) = bit_defaults_session().await;

    assert!(
        rows(
            &mut s,
            "SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) \
             FROM pg_attrdef d \
             JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
             JOIN pg_class c ON c.oid = d.adrelid \
             WHERE c.relname = 'bit_defaults' ORDER BY a.attnum",
        )
        .await
            == vec![
                vec![Some("b1".into()), Some("'1001'::\"bit\"".into())],
                vec![Some("b2".into()), Some("'0101'::\"bit\"".into())],
                vec![Some("b3".into()), Some("'1001'::bit varying".into())],
                vec![Some("b4".into()), Some("'0101'::\"bit\"".into())],
            ]
    );
}

/// `ALTER TABLE ... SET DEFAULT` is a second raise site for the same rule, and
/// a column that gets its default that way has to deparse like one that was
/// born with it — otherwise the two spellings of the same table disagree.
#[tokio::test]
async fn alter_table_set_default_keeps_the_written_literals_type_too() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE bit_altered(b bit varying(5))").await;
    run(
        &mut s,
        "ALTER TABLE bit_altered ALTER COLUMN b SET DEFAULT B'0101'",
    )
    .await;

    assert!(
        rows(
            &mut s,
            "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d \
             JOIN pg_class c ON c.oid = d.adrelid WHERE c.relname = 'bit_altered'",
        )
        .await
            == vec![vec![Some("'0101'::\"bit\"".into())]]
    );

    run(
        &mut s,
        "ALTER TABLE bit_altered ALTER COLUMN b SET DEFAULT '1001'",
    )
    .await;
    assert!(
        rows(
            &mut s,
            "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d \
             JOIN pg_class c ON c.oid = d.adrelid WHERE c.relname = 'bit_altered'",
        )
        .await
            == vec![vec![Some("'1001'::bit varying".into())]]
    );

    run(&mut s, "INSERT INTO bit_altered DEFAULT VALUES").await;
    assert!(rows(&mut s, "TABLE bit_altered").await == vec![vec![Some("1001".into())]]);
}

/// Keeping the written datum must move the LABEL and nothing else: the value
/// each default inserts, and the type the column reports, are what they were.
#[tokio::test]
async fn keeping_the_written_literal_changes_no_stored_value_or_column_type() {
    let (_engine, mut s) = bit_defaults_session().await;
    run(&mut s, "INSERT INTO bit_defaults DEFAULT VALUES").await;

    assert!(
        rows(&mut s, "TABLE bit_defaults").await
            == vec![vec![
                Some("1001".into()),
                Some("0101".into()),
                Some("1001".into()),
                Some("0101".into()),
            ]]
    );
    assert!(
        rows(
            &mut s,
            "SELECT pg_typeof(b1), pg_typeof(b2), pg_typeof(b3), pg_typeof(b4) \
             FROM bit_defaults",
        )
        .await
            == vec![vec![
                Some("bit".into()),
                Some("bit".into()),
                Some("bit varying".into()),
                Some("bit varying".into()),
            ]]
    );
}

/// The written datum is kept only where the coercion changed no bits. A length
/// coercion does change bits, and `bit(n)` demands an exact length, so a
/// too-short `B'...'` is still the 22026 `PostgreSQL` raises rather than a
/// default that quietly keeps the wrong width.
#[tokio::test]
async fn a_length_coercion_is_still_applied_to_a_bit_string_default() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();

    let e = s
        .simple_query("CREATE TABLE bit_narrow(a bit(5) DEFAULT B'101')")
        .await
        .expect_err("a short bit literal should not fit bit(5)");
    assert!(
        (e.code.as_str(), e.message.as_str())
            == ("22026", "bit string length 3 does not match type bit(5)")
    );
}

/// Defaults of every other type still store the COERCED value. `numeric`'s
/// scale and `bpchar`'s padding are both applied by the coercion this change
/// steps around for bit strings, so they are the ones to watch.
#[tokio::test]
async fn a_default_of_another_type_is_still_stored_coerced() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE other_defaults(n numeric(10,2) DEFAULT 1.5, i int8 DEFAULT 7)",
    )
    .await;
    run(&mut s, "INSERT INTO other_defaults DEFAULT VALUES").await;

    assert!(
        rows(&mut s, "TABLE other_defaults").await
            == vec![vec![Some("1.50".into()), Some("7".into())]]
    );
    assert!(
        rows(
            &mut s,
            "SELECT pg_typeof(n), pg_typeof(i) FROM other_defaults",
        )
        .await
            == vec![vec![Some("numeric".into()), Some("bigint".into())]]
    );
}
