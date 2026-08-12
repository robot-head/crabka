//! S6: how `EXPLAIN` spells an aggregate call that carries `DISTINCT`, its own
//! `ORDER BY`, or a `FILTER` clause.
//!
//! Every expected string here is `PostgreSQL` 18.4's own `EXPLAIN (COSTS OFF)`
//! text, captured from the pinned oracle over a table of the same shape. The
//! assertions are deliberately on the deparsed `Filter:` line rather than the
//! whole plan: `PostgreSQL` also switches `HashAggregate` to `GroupAggregate`
//! and plants a pre-aggregation `Sort` for an ordered or distinct aggregate,
//! and Gres does neither — it sorts inside each accumulator instead. That
//! divergence is a strategy difference recorded in `docs/PG_COMPAT_MATRIX.md`,
//! not a spelling one, and inventing the nodes here would make `EXPLAIN` claim
//! a plan the engine does not run.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// The plan `EXPLAIN (COSTS OFF)` prints for `sql`, one string per line.
async fn plan(engine: &SqlEngine, sql: &str) -> Vec<String> {
    let results = engine
        .connect()
        .simple_query(&format!("EXPLAIN (COSTS OFF) {sql}"))
        .await
        .expect("EXPLAIN succeeds");
    match results.into_iter().next_back() {
        Some(QueryResult::Rows { rows, .. }) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        cell.as_ref().map_or_else(String::new, |cell: &Cell| {
                            String::from_utf8(cell.text.to_vec()).expect("valid text cell")
                        })
                    })
                    .collect::<String>()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The single `Filter:` detail line of a plan, trimmed of its indentation.
async fn filter_line(engine: &SqlEngine, sql: &str) -> String {
    let lines = plan(engine, sql).await;
    let mut filters = lines
        .iter()
        .map(|line| line.trim())
        .filter(|line| line.starts_with("Filter: "));
    let found = filters
        .next()
        .unwrap_or_else(|| panic!("no Filter line in {lines:?} for `{sql}`"))
        .to_string();
    assert!(filters.next().is_none(), "more than one Filter line: {sql}");
    found
}

async fn fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    for sql in [
        "CREATE TABLE t (a int4, b int4, c text, d int4)",
        "INSERT INTO t VALUES (1, 1, 'x', 1), (2, 1, 'y', 2), (3, 2, 'z', 3)",
    ] {
        engine.connect().simple_query(sql).await.expect(sql);
    }
    engine
}

/// The whole modifier matrix, grouped so the aggregate lands on a
/// `HashAggregate`'s `Filter` line.
#[tokio::test]
async fn a_grouped_having_prints_every_aggregate_modifier() {
    let engine = fixture().await;
    let cases: &[(&str, &str)] = &[
        // DISTINCT alone.
        ("count(distinct a) > 1", "Filter: (count(DISTINCT a) > 1)"),
        // FILTER alone.
        (
            "count(*) filter (where a > 0) > 1",
            "Filter: (count(*) FILTER (WHERE (a > 0)) > 1)",
        ),
        // The aggregate's own ORDER BY alone.
        (
            "string_agg(c, ',' order by a) > 'x'",
            "Filter: (string_agg(c, ','::text ORDER BY a) > 'x'::text)",
        ),
        // DISTINCT + FILTER.
        (
            "count(distinct a) filter (where a > 0) > 1",
            "Filter: (count(DISTINCT a) FILTER (WHERE (a > 0)) > 1)",
        ),
        // DISTINCT + ORDER BY.
        (
            "string_agg(distinct c, ',' order by c) > 'x'",
            "Filter: (string_agg(DISTINCT c, ','::text ORDER BY c) > 'x'::text)",
        ),
        // ORDER BY + FILTER.
        (
            "string_agg(c, ',' order by a) filter (where a > 0) > 'x'",
            "Filter: (string_agg(c, ','::text ORDER BY a) FILTER (WHERE (a > 0)) > 'x'::text)",
        ),
        // All three at once.
        (
            "string_agg(distinct c, ',' order by c) filter (where a > 0) > 'x'",
            "Filter: (string_agg(DISTINCT c, ','::text ORDER BY c) FILTER (WHERE (a > 0)) > 'x'::text)",
        ),
        // Several sort keys, mixed direction, explicit NULLS on both. PostgreSQL
        // prints a NULLS clause only where it is not the direction's default, so
        // `a ASC` and `d DESC` keep no clause but `b ASC NULLS FIRST` and
        // `c DESC NULLS LAST` do — and `ASC` itself never prints.
        (
            "string_agg(c, ',' order by a asc, d desc, b asc nulls first, c desc nulls last) > 'x'",
            "Filter: (string_agg(c, ','::text ORDER BY a, d DESC, b NULLS FIRST, c DESC NULLS LAST) > 'x'::text)",
        ),
        // count(*) unmodified, and count(*) with a FILTER over a non-operator
        // predicate.
        ("count(*) > 1", "Filter: (count(*) > 1)"),
        (
            "count(*) filter (where a is not null) > 1",
            "Filter: (count(*) FILTER (WHERE (a IS NOT NULL)) > 1)",
        ),
    ];
    for (having, expected) in cases {
        let sql = format!("SELECT b FROM t GROUP BY b HAVING {having}");
        assert!(filter_line(&engine, &sql).await == *expected, "{having}");
    }
}

/// An ungrouped `HAVING` is applied by the `Aggregate` node, so it prints there
/// too. This line was absent altogether: the branch that builds the ungrouped
/// `Aggregate` never looked at `HAVING`, so `EXPLAIN` described a plan that
/// returns the aggregate unconditionally.
#[tokio::test]
async fn an_ungrouped_having_prints_on_the_aggregate_node() {
    let engine = fixture().await;
    assert!(
        plan(&engine, "SELECT count(*) FROM t HAVING count(*) > 1").await
            == [
                "Aggregate",
                "  Filter: (count(*) > 1)",
                "  ->  Seq Scan on t"
            ]
    );
    assert!(
        plan(
            &engine,
            "SELECT count(*) FROM t HAVING count(*) filter (where a > 0) > 1"
        )
        .await
            == [
                "Aggregate",
                "  Filter: (count(*) FILTER (WHERE (a > 0)) > 1)",
                "  ->  Seq Scan on t"
            ]
    );
}

/// The modifiers reach a `Sort Key` too, which is rendered by a different
/// caller of the same deparser.
///
/// The outer parentheses are the `Sort` node's own: it evaluates nothing, so
/// its key is a reference to the aggregate below and prints wrapped. The
/// aggregate's *inner* `ORDER BY` never picks them up, because no target list
/// stands between the aggregate and its own sort expressions.
#[tokio::test]
async fn an_aggregate_in_order_by_carries_its_modifiers_into_the_sort_key() {
    let engine = fixture().await;
    let cases: &[(&str, &str)] = &[
        (
            "count(*) filter (where a > 0)",
            "Sort Key: (count(*) FILTER (WHERE (a > 0)))",
        ),
        ("count(*)", "Sort Key: (count(*))"),
        (
            "string_agg(c, ',' ORDER BY a DESC)",
            "Sort Key: (string_agg(c, ','::text ORDER BY a DESC))",
        ),
    ];
    for (order_by, expected) in cases {
        let lines = plan(
            &engine,
            &format!("SELECT b FROM t GROUP BY b ORDER BY {order_by}"),
        )
        .await;
        assert!(
            lines.iter().any(|line| line.trim() == *expected),
            "{order_by}: {lines:?}"
        );
    }
}

// --------------------------------------------------------- shared machinery
//
// `deparse_bare_with` renders every expression EXPLAIN prints, so widening its
// function arm is exactly the change that can alter output with no aggregate in
// it at all. These cases carry no aggregate and no modifier.

/// An ordinary scalar call must still print as bare `name(args)`.
#[tokio::test]
async fn a_plain_function_call_is_unchanged_by_the_modifier_rendering() {
    let engine = fixture().await;
    let cases: &[(&str, &str)] = &[
        ("upper(c) = 'A'", "Filter: (upper(c) = 'A'::text)"),
        (
            "upper(lower(c)) = 'A'",
            "Filter: (upper(lower(c)) = 'A'::text)",
        ),
        ("abs(a) > 1", "Filter: (abs(a) > 1)"),
    ];
    for (predicate, expected) in cases {
        let sql = format!("SELECT * FROM t WHERE {predicate}");
        assert!(filter_line(&engine, &sql).await == *expected, "{predicate}");
    }
}

/// A query-level `ORDER BY` is rendered by the same key list an aggregate's own
/// `ORDER BY` now uses, so its direction and NULLS spelling must not move.
#[tokio::test]
async fn a_query_level_sort_key_is_unchanged() {
    let engine = fixture().await;
    let lines = plan(
        &engine,
        "SELECT * FROM t ORDER BY a DESC NULLS LAST, b NULLS FIRST",
    )
    .await;
    assert!(lines[0] == "Sort");
    assert!(lines[1] == "  Sort Key: a DESC NULLS LAST, b NULLS FIRST");
}

/// A `GROUP BY` over a function call goes through the same arm and keeps its
/// unparenthesised `Group Key` spelling.
#[tokio::test]
async fn a_group_key_over_a_function_call_is_unchanged() {
    let engine = fixture().await;
    let lines = plan(&engine, "SELECT upper(c) FROM t GROUP BY upper(c)").await;
    assert!(lines[0] == "HashAggregate");
    assert!(lines[1] == "  Group Key: upper(c)");
}

/// A window call is held outside the expression tree, with its own `FILTER` and
/// `OVER` on [`crabka_pgparser::ast::WindowCall`] rather than on a `FuncCall`,
/// so it is rendered by a different path entirely. Widening the aggregate arm
/// must not start printing window syntax through it.
#[tokio::test]
async fn a_window_call_does_not_render_through_the_aggregate_arm() {
    let engine = fixture().await;
    for sql in [
        "SELECT b, count(*) over (partition by b order by a) FROM t",
        "SELECT b, count(*) filter (where a > 0) over (partition by b) FROM t",
    ] {
        let text = plan(&engine, sql).await.join("\n");
        assert!(!text.contains("FILTER (WHERE"), "{sql} => {text}");
        assert!(!text.contains("OVER"), "{sql} => {text}");
    }
}
