//! A correlated subquery can name an outer relation's whole row.
//!
//! `SELECT t FROM t` resolves a bare name that matches no column against the
//! range table, and a match there is the entire row as one composite value.
//! Inside a correlated subquery the reference has to be *substituted* rather
//! than resolved in place — the subquery is rewritten per outer row — and the
//! walker that does the substituting had no whole-row fallback, so the same
//! reference that answered at the top level was 42703 one level down.
//!
//! `IS NULL` over a composite is field-wise, which is the rule that separates a
//! stored row of NULLs from a row an outer join invented. Both render, and only
//! one of them is a composite at all.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

/// Every cell of the result, row by row, with NULL distinguishable from an
/// empty string — which is the whole point of several of these cases.
async fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &session
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))[0]
    {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell: &Option<Cell>| {
                        cell.as_ref()
                            .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn sqlstate(session: &mut SqlSession, sql: &str) -> String {
    match session.simple_query(sql).await {
        Err(error) => error.code,
        Ok(ok) => panic!("expected {sql} to fail, got {ok:?}"),
    }
}

fn text(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

async fn fixture() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for ddl in [
        "CREATE TABLE ca (x int)",
        "CREATE TABLE cb (y int)",
        "INSERT INTO ca VALUES (1), (2)",
        "INSERT INTO cb VALUES (10), (20), (30)",
    ] {
        run(&mut session, ddl).await;
    }
    (engine, session)
}

/// The reference reaches every shape a correlated subquery takes — a scalar
/// sub-select in the projection, one with its own FROM, an `EXISTS`, and a
/// `LATERAL` item — because all four go through the one substituting walk.
#[tokio::test]
async fn a_correlated_subquery_resolves_an_outer_relations_whole_row() {
    let (_engine, mut session) = fixture().await;

    // A scalar sub-select whose FROM is unrelated: the outer row is the only
    // thing `ca` can mean there.
    assert!(
        rows(
            &mut session,
            "SELECT ca.x, (SELECT count(*) FROM cb WHERE ca IS NOT NULL) FROM ca ORDER BY 1",
        )
        .await
            == vec![text(&["1", "3"]), text(&["2", "3"])]
    );

    // A sub-select with no FROM at all, rendering the composite.
    assert!(
        rows(&mut session, "SELECT (SELECT ca::text) FROM ca ORDER BY 1").await
            == vec![text(&["(1)"]), text(&["(2)"])]
    );

    // The field names travel with the value, which is what row_to_json reads.
    assert!(
        rows(
            &mut session,
            "SELECT (SELECT row_to_json(ca) FROM cb LIMIT 1) FROM ca ORDER BY x",
        )
        .await
            == vec![text(&["{\"x\":1}"]), text(&["{\"x\":2}"])]
    );

    // EXISTS and LATERAL take the same walk.
    assert!(
        rows(
            &mut session,
            "SELECT x FROM ca WHERE EXISTS (SELECT 1 FROM cb WHERE ca IS NOT NULL) ORDER BY 1",
        )
        .await
            == vec![text(&["1"]), text(&["2"])]
    );
    assert!(
        rows(
            &mut session,
            "SELECT ca.x, l.c FROM ca, LATERAL (SELECT count(*) c FROM cb WHERE ca IS NOT NULL) l \
             ORDER BY 1",
        )
        .await
            == vec![text(&["1", "3"]), text(&["2", "3"])]
    );
}

/// What must keep losing to something else: an inner relation of the same name,
/// a column of that name, a qualified spelling, and a name nothing holds. Each
/// is a different reason the fallback must not fire.
#[tokio::test]
async fn the_outer_whole_row_loses_to_everything_that_outranks_it() {
    let (_engine, mut session) = fixture().await;

    // An inner relation of the same name is what PostgreSQL binds, so this
    // counts the inner `ca`'s two rows, not the outer row's presence.
    assert!(
        rows(
            &mut session,
            "SELECT (SELECT count(*) FROM ca WHERE ca IS NOT NULL) FROM ca ORDER BY 1",
        )
        .await
            == vec![text(&["2"]), text(&["2"])]
    );

    // A column always outranks the relation.
    run(&mut session, "CREATE TABLE shadow (shadow int)").await;
    run(&mut session, "INSERT INTO shadow VALUES (7), (NULL)").await;
    assert!(
        rows(
            &mut session,
            "SELECT (SELECT shadow) FROM shadow ORDER BY 1"
        )
        .await
            == vec![text(&["7"]), vec![None]]
    );

    // `s.t` is read as "column t of range-table entry s", never as the whole
    // row of `s.t`, so it reports the missing FROM entry for `s`.
    assert!(
        sqlstate(
            &mut session,
            "SELECT (SELECT count(*) FROM cb WHERE public.ca IS NOT NULL) FROM ca",
        )
        .await
            == "42P01"
    );

    // A bare name that is neither a column nor a relation keeps its 42703.
    assert!(
        sqlstate(
            &mut session,
            "SELECT (SELECT count(*) FROM cb WHERE nosuch IS NOT NULL) FROM ca",
        )
        .await
            == "42703"
    );
}

/// `IS NULL` over a composite is field-wise — `PostgreSQL` sets `argisrow` from
/// the operand's *type* — so a row with one NULL field satisfies neither
/// `IS NULL` nor `IS NOT NULL`. `IS DISTINCT FROM NULL` is the test that does
/// separate a stored row from an absent one, and the rendering shows the same
/// split.
#[tokio::test]
async fn is_null_on_a_correlated_whole_row_is_field_wise() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE cn (a int, b int)").await;
    run(
        &mut session,
        "INSERT INTO cn VALUES (NULL, NULL), (1, NULL), (1, 2)",
    )
    .await;

    assert!(
        rows(
            &mut session,
            "SELECT (SELECT cn IS NULL), (SELECT cn IS NOT NULL), \
                    (SELECT cn IS DISTINCT FROM NULL), (SELECT cn::text) \
               FROM cn ORDER BY a NULLS FIRST, b NULLS FIRST",
        )
        .await
            == vec![
                text(&["t", "f", "t", "(,)"]),
                text(&["f", "f", "t", "(1,)"]),
                text(&["f", "t", "t", "(1,2)"]),
            ]
    );
}

/// The discriminating case: a stored `(NULL, NULL)` row and a row an outer join
/// invented, in one result set. `IS NULL` is true for both and cannot tell them
/// apart; the composite renders `(,)` and the invented one is NULL outright.
#[tokio::test]
async fn a_stored_all_null_row_and_an_invented_one_stay_distinguishable() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for ddl in [
        "CREATE TABLE jl (k int)",
        "CREATE TABLE jr (a int, b int)",
        "INSERT INTO jl VALUES (1), (2)",
        // Only k=1 has a match, and the row it matches is all NULLs.
        "INSERT INTO jr VALUES (NULL, NULL)",
    ] {
        run(&mut session, ddl).await;
    }

    // k=1 joins the stored row of NULLs; k=2 is null-extended and has no whole
    // row at all. Both are `IS NULL`, and only the rendering separates them.
    assert!(
        rows(
            &mut session,
            "SELECT jl.k, (SELECT jr::text), (SELECT jr IS NULL), \
                    (SELECT jr IS DISTINCT FROM NULL) \
               FROM jl LEFT JOIN jr ON jl.k = 1 ORDER BY 1",
        )
        .await
            == vec![
                text(&["1", "(,)", "t", "t"]),
                vec![Some("2".into()), None, Some("t".into()), Some("f".into())],
            ]
    );
}

/// The same field-wise rule reached through a grouped evaluation, which is a
/// separate evaluator from the scalar one and had the same gap.
#[tokio::test]
async fn a_composite_is_null_is_field_wise_under_grouping() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TYPE ct AS (a int, b int)").await;
    run(&mut session, "CREATE TABLE ct_t (k int, c ct)").await;
    run(
        &mut session,
        "INSERT INTO ct_t VALUES (1, ROW(NULL, NULL)::ct), (2, NULL), (3, ROW(1, NULL)::ct), \
         (4, ROW(1, 2)::ct)",
    )
    .await;

    // A composite column obeys `argisrow` too, with no whole-row reference and
    // no subquery anywhere in sight.
    assert!(
        rows(
            &mut session,
            "SELECT k, c IS NULL, c IS NOT NULL FROM ct_t ORDER BY k",
        )
        .await
            == vec![
                text(&["1", "t", "f"]),
                text(&["2", "t", "f"]),
                text(&["3", "f", "f"]),
                text(&["4", "f", "t"]),
            ]
    );

    // GROUP BY routes the same test through the grouped evaluator.
    assert!(
        rows(
            &mut session,
            "SELECT k, c IS NULL FROM ct_t GROUP BY k, c ORDER BY k",
        )
        .await
            == vec![
                text(&["1", "t"]),
                text(&["2", "t"]),
                text(&["3", "f"]),
                text(&["4", "f"]),
            ]
    );
}
