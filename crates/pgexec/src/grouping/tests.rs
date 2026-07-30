use assert2::assert;
use crabka_pgwire::engine::{Engine, QueryResult, Session};

use super::*;
use crate::SqlEngine;

/// Run `setup` then `sql`, returning each output row as its text cells.
async fn rows(setup: &[&str], sql: &str) -> Vec<Vec<Option<String>>> {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for statement in setup {
        session.simple_query(statement).await.expect("setup");
    }
    let results = session.simple_query(sql).await.expect("query");
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("expected rows from {sql}")
    };
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_ref()
                        .map(|c| String::from_utf8(c.text.to_vec()).expect("utf-8"))
                })
                .collect()
        })
        .collect()
}

async fn sqlstate(setup: &[&str], sql: &str) -> String {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for statement in setup {
        session.simple_query(statement).await.expect("setup");
    }
    session
        .simple_query(sql)
        .await
        .expect_err("expected an error")
        .code
}

const SETUP: &[&str] = &[
    "CREATE TABLE gs (a int4, b int4, v int4)",
    "INSERT INTO gs VALUES (1,1,10),(1,2,20),(2,1,30),(NULL,1,40)",
];

fn cells(rows: &[&[&str]]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|c| (*c != "NULL").then(|| (*c).to_string()))
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn grouping_set_shapes_match_postgres() {
    // (sql, expected rows) — every query orders totally so the comparison is exact.
    let cases: Vec<(&str, Vec<Vec<Option<String>>>)> = vec![
        (
            "SELECT a, b, count(*) FROM gs GROUP BY ROLLUP(a, b) ORDER BY a, b, 3",
            cells(&[
                &["1", "1", "1"],
                &["1", "2", "1"],
                &["1", "NULL", "2"],
                &["2", "1", "1"],
                &["2", "NULL", "1"],
                &["NULL", "1", "1"],
                &["NULL", "NULL", "1"],
                &["NULL", "NULL", "4"],
            ]),
        ),
        (
            "SELECT a, b, count(*) FROM gs GROUP BY CUBE(a, b) ORDER BY a, b, 3",
            cells(&[
                &["1", "1", "1"],
                &["1", "2", "1"],
                &["1", "NULL", "2"],
                &["2", "1", "1"],
                &["2", "NULL", "1"],
                &["NULL", "1", "1"],
                &["NULL", "1", "3"],
                &["NULL", "2", "1"],
                &["NULL", "NULL", "1"],
                &["NULL", "NULL", "4"],
            ]),
        ),
        ("SELECT count(*) FROM gs GROUP BY ()", cells(&[&["4"]])),
        (
            "SELECT a, count(*) FROM gs GROUP BY GROUPING SETS (a, a) ORDER BY a, 2",
            cells(&[
                &["1", "2"],
                &["1", "2"],
                &["2", "1"],
                &["2", "1"],
                &["NULL", "1"],
                &["NULL", "1"],
            ]),
        ),
        (
            "SELECT a, count(*) FROM gs GROUP BY DISTINCT ROLLUP(a), ROLLUP(a) ORDER BY a, 2",
            cells(&[&["1", "2"], &["2", "1"], &["NULL", "1"], &["NULL", "4"]]),
        ),
        // The grand total still appears when the input is empty.
        (
            "SELECT a, count(*) FROM gs WHERE false GROUP BY ROLLUP(a)",
            cells(&[&["NULL", "0"]]),
        ),
        (
            "SELECT a, count(*) FROM gs WHERE false GROUP BY GROUPING SETS ((a))",
            Vec::new(),
        ),
        // An aggregate still sees the real values in an aggregated row.
        (
            "SELECT a, sum(a), count(a) FROM gs GROUP BY ROLLUP(a) ORDER BY a, 2",
            cells(&[
                &["1", "2", "2"],
                &["2", "2", "1"],
                &["NULL", "4", "3"],
                &["NULL", "NULL", "0"],
            ]),
        ),
        // An expression over a grouping column reads the grouped NULL.
        (
            "SELECT a + 1, count(*) FROM gs GROUP BY ROLLUP(a) ORDER BY 1, 2",
            cells(&[&["2", "2"], &["3", "1"], &["NULL", "1"], &["NULL", "4"]]),
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(SETUP, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn grouping_function_reports_the_aggregated_columns() {
    let got = rows(
        SETUP,
        "SELECT grouping(a) ga, grouping(b) gb, grouping(a, b) gab, grouping(b, a) gba, count(*) \
         FROM gs GROUP BY GROUPING SETS ((a, b), (a), (b), ()) ORDER BY gab, ga, gb, 5",
    )
    .await;
    let masks: Vec<Vec<Option<String>>> = got.into_iter().map(|row| row[..4].to_vec()).collect();
    // (a,b) -> 0/0; (a) -> b aggregated; (b) -> a aggregated; () -> both.
    assert!(masks[0] == cells(&[&["0", "0", "0", "0"]])[0]);
    assert!(masks.contains(&cells(&[&["0", "1", "1", "2"]])[0]));
    assert!(masks.contains(&cells(&[&["1", "0", "2", "1"]])[0]));
    assert!(masks.contains(&cells(&[&["1", "1", "3", "3"]])[0]));
}

#[tokio::test]
async fn group_by_output_references_resolve() {
    let by_ordinal = rows(
        SETUP,
        "SELECT a AS ka, count(*) FROM gs GROUP BY 1 ORDER BY 1",
    )
    .await;
    let by_label = rows(
        SETUP,
        "SELECT a AS ka, count(*) FROM gs GROUP BY ka ORDER BY 1",
    )
    .await;
    let by_column = rows(
        SETUP,
        "SELECT a AS ka, count(*) FROM gs GROUP BY a ORDER BY 1",
    )
    .await;
    assert!(by_ordinal == by_column);
    assert!(by_label == by_column);
    assert!(by_column == cells(&[&["1", "2"], &["2", "1"], &["NULL", "1"]]));
}

#[tokio::test]
async fn grouping_errors_use_postgres_sqlstates() {
    let cases = [
        // GROUPING over something that is not a grouping expression.
        ("SELECT a, grouping(v) FROM gs GROUP BY ROLLUP(a)", "42803"),
        ("SELECT grouping(a) FROM gs", "42803"),
        // A projected column that no grouping set covers.
        ("SELECT v, count(*) FROM gs GROUP BY ROLLUP(a)", "42803"),
        // Output-position references out of range.
        ("SELECT a, count(*) FROM gs GROUP BY 5", "42P10"),
        ("SELECT a, count(*) FROM gs GROUP BY 0", "42P10"),
        // An aggregate may not be a grouping expression.
        ("SELECT count(*) FROM gs GROUP BY ROLLUP(count(*))", "42803"),
        // CUBE's element-count cap.
        (
            "SELECT count(*) FROM gs GROUP BY CUBE(a,b,v,a,b,v,a,b,v,a,b,v,a)",
            "54011",
        ),
    ];
    for (sql, code) in cases {
        assert!(sqlstate(SETUP, sql).await == code, "{sql}");
    }
}

#[test]
fn rollup_expands_to_its_prefixes_longest_first() {
    let clause = GroupingClause {
        distinct: false,
        items: vec![GroupItem::Rollup(vec![
            GroupItem::Expr(0),
            GroupItem::Expr(1),
        ])],
    };
    assert!(expand(&clause) == vec![vec![0, 1], vec![0], vec![]]);
}

#[test]
fn cube_expands_to_every_subset() {
    let clause = GroupingClause {
        distinct: false,
        items: vec![GroupItem::Cube(vec![
            GroupItem::Expr(0),
            GroupItem::Expr(1),
        ])],
    };
    let sets = expand(&clause);
    assert!(sets.len() == 4);
    for expected in [vec![0, 1], vec![0], vec![1], Vec::new()] {
        assert!(sets.contains(&expected), "missing {expected:?}");
    }
}

#[test]
fn items_combine_by_cross_product_and_distinct_dedups() {
    let items = vec![
        GroupItem::Rollup(vec![GroupItem::Expr(0), GroupItem::Expr(1)]),
        GroupItem::Rollup(vec![GroupItem::Expr(0)]),
    ];
    let all = expand(&GroupingClause {
        distinct: false,
        items: items.clone(),
    });
    // {a,b}x{a} and {a,b}x{} both collapse to {a,b}; {a}x{a}, {a}x{} and {}x{a}
    // all collapse to {a}; {}x{} is the grand total.
    assert!(all == vec![vec![0, 1], vec![0, 1], vec![0], vec![0], vec![0], vec![]]);
    let distinct = expand(&GroupingClause {
        distinct: true,
        items,
    });
    assert!(distinct == vec![vec![0, 1], vec![0], vec![]]);
}

#[test]
fn composite_elements_join_and_leave_a_set_together() {
    let clause = GroupingClause {
        distinct: false,
        items: vec![GroupItem::Rollup(vec![
            GroupItem::Composite(vec![0, 1]),
            GroupItem::Expr(2),
        ])],
    };
    assert!(expand(&clause) == vec![vec![0, 1, 2], vec![0, 1], vec![]]);
}

#[test]
fn grouping_mask_sets_a_bit_per_aggregated_argument_msb_first() {
    // GROUPING(a, b) over the set {a}: `b` is aggregated, so only the low bit.
    assert!(grouping_mask(&[0, 1], &[0]) == 1);
    assert!(grouping_mask(&[0, 1], &[1]) == 2);
    assert!(grouping_mask(&[0, 1], &[0, 1]) == 0);
    assert!(grouping_mask(&[0, 1], &[]) == 3);
    // Argument order, not group-by order, decides the bit positions.
    assert!(grouping_mask(&[1, 0], &[1]) == 1);
}

/// A window function runs ABOVE the grouping, so it sees one row per
/// grouping-set group. Before the window path consulted the grouping-set
/// rewrite it silently dropped the clause and returned the plain grouped rows.
#[tokio::test]
async fn window_functions_run_over_the_expanded_grouping_sets() {
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "SELECT a, sum(v), count(*) OVER () FROM gs GROUP BY ROLLUP(a) ORDER BY a, 2",
            &[
                &["1", "30", "4"],
                &["2", "30", "4"],
                &["NULL", "40", "4"],
                &["NULL", "100", "4"],
            ],
        ),
        (
            "SELECT a, sum(v), count(*) OVER () FROM gs GROUP BY GROUPING SETS ((a), ()) \
             ORDER BY a, 2",
            &[
                &["1", "30", "4"],
                &["2", "30", "4"],
                &["NULL", "40", "4"],
                &["NULL", "100", "4"],
            ],
        ),
        // The empty grouping set folds the input to one row before the window.
        (
            "SELECT count(*) OVER () FROM gs GROUP BY GROUPING SETS (())",
            &[&["1"]],
        ),
        // GROUPING() is usable in a windowed grouping-set query, in the select
        // list, in a window spec, and in ORDER BY.
        (
            "SELECT grouping(a), count(*) OVER (PARTITION BY grouping(a)) FROM gs \
             GROUP BY ROLLUP(a) ORDER BY grouping(a), a",
            &[&["0", "3"], &["0", "3"], &["0", "3"], &["1", "1"]],
        ),
        // A SQL92 output reference in GROUP BY names the ORIGINAL select list.
        (
            "SELECT a, count(*) OVER () FROM gs GROUP BY 1 ORDER BY a",
            &[&["1", "3"], &["2", "3"], &["NULL", "3"]],
        ),
        (
            "SELECT a AS k, count(*) OVER () FROM gs GROUP BY k ORDER BY 1",
            &[&["1", "3"], &["2", "3"], &["NULL", "3"]],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(SETUP, sql).await == cells(expected), "{sql}");
    }
}

/// Over an empty input only an empty grouping set produces a group — and the
/// window node then sees exactly those rows, one per such set.
#[tokio::test]
async fn window_functions_over_an_empty_grouping_set_input() {
    let setup: &[&str] = &["CREATE TABLE e (a int4, v int4)"];
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "SELECT a, sum(v), count(*) OVER () FROM e GROUP BY ROLLUP(a) ORDER BY a",
            &[&["NULL", "NULL", "1"]],
        ),
        (
            "SELECT sum(v), count(*) OVER () FROM e GROUP BY GROUPING SETS ((), ())",
            &[&["NULL", "2"], &["NULL", "2"]],
        ),
        (
            "SELECT a, count(*) OVER () FROM e GROUP BY ROLLUP(a, a) ORDER BY a",
            &[&["NULL", "1"]],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(setup, sql).await == cells(expected), "{sql}");
    }
}

/// `DISTINCT ON` runs over the GROUPED output. It used to be ignored entirely on
/// the aggregate path, which returned every group.
#[tokio::test]
async fn distinct_on_dedups_the_grouped_output() {
    let cases: &[(&str, &[&[&str]])] = &[
        // Plain grouped path: two groups share a=1, only the first survives.
        (
            "SELECT DISTINCT ON (a) a, sum(v) FROM gs GROUP BY a, b ORDER BY a, sum(v)",
            &[&["1", "10"], &["2", "30"], &["NULL", "40"]],
        ),
        (
            "SELECT DISTINCT ON (a) a, sum(v) FROM gs GROUP BY a, b ORDER BY a, sum(v) DESC",
            &[&["1", "20"], &["2", "30"], &["NULL", "40"]],
        ),
        // Grouping-set path: the ON key is a grouping expression, so it reads the
        // set's own (possibly NULL) key rather than the source column.
        (
            "SELECT DISTINCT ON (a) a, sum(v) FROM gs GROUP BY ROLLUP(a) ORDER BY a, sum(v)",
            &[&["1", "30"], &["2", "30"], &["NULL", "40"]],
        ),
        (
            "SELECT DISTINCT ON (grouping(a)) grouping(a), a FROM gs GROUP BY ROLLUP(a) \
             ORDER BY grouping(a), a",
            &[&["0", "1"], &["1", "NULL"]],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(SETUP, sql).await == cells(expected), "{sql}");
    }
}

/// A grouping expression is matched by the column the reference resolves to, not
/// by how it was spelled, so `*` and a table-qualified reference are both
/// grouped-valid — both used to be 42803.
#[tokio::test]
async fn qualified_and_wildcard_references_match_the_grouping_expressions() {
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "SELECT * FROM gs GROUP BY a, b, v ORDER BY a, b, v",
            &[
                &["1", "1", "10"],
                &["1", "2", "20"],
                &["2", "1", "30"],
                &["NULL", "1", "40"],
            ],
        ),
        (
            "SELECT gs.a, sum(v) FROM gs GROUP BY ROLLUP(a) ORDER BY 1, 2",
            &[
                &["1", "30"],
                &["2", "30"],
                &["NULL", "40"],
                &["NULL", "100"],
            ],
        ),
        (
            "SELECT a, sum(v) FROM gs GROUP BY ROLLUP(gs.a) ORDER BY 1, 2",
            &[
                &["1", "30"],
                &["2", "30"],
                &["NULL", "40"],
                &["NULL", "100"],
            ],
        ),
        (
            "SELECT * FROM gs GROUP BY ROLLUP(a, b, v) ORDER BY grouping(a, b, v), a, b, v",
            &[
                &["1", "1", "10"],
                &["1", "2", "20"],
                &["2", "1", "30"],
                &["NULL", "1", "40"],
                &["1", "1", "NULL"],
                &["1", "2", "NULL"],
                &["2", "1", "NULL"],
                &["NULL", "1", "NULL"],
                &["1", "NULL", "NULL"],
                &["2", "NULL", "NULL"],
                &["NULL", "NULL", "NULL"],
                &["NULL", "NULL", "NULL"],
            ],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(SETUP, sql).await == cells(expected), "{sql}");
    }
}

/// `PostgreSQL` names the ungrouped column by its range-table alias.
#[tokio::test]
async fn an_ungrouped_column_is_reported_with_its_qualifier() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for statement in SETUP {
        session.simple_query(statement).await.expect("setup");
    }
    let error = session
        .simple_query("SELECT * FROM gs GROUP BY a")
        .await
        .expect_err("ungrouped b");
    assert!(error.code == "42803");
    assert!(
        error.message
            == "column \"gs.b\" must appear in the GROUP BY clause or be used in an aggregate \
                function"
    );
}

/// `GROUPING(…)` has no meaning below the grouping, and `PostgreSQL` names the
/// clause it was found in.
#[tokio::test]
async fn grouping_below_the_grouping_names_its_clause() {
    let cases = [
        (
            "SELECT a FROM gs WHERE grouping(a) = 0 GROUP BY ROLLUP(a)",
            "grouping operations are not allowed in WHERE",
        ),
        (
            "SELECT a FROM gs GROUP BY grouping(a)",
            "grouping operations are not allowed in GROUP BY",
        ),
        (
            "SELECT x.a FROM gs x JOIN gs y ON grouping(x.a) = 0 GROUP BY x.a",
            "grouping operations are not allowed in JOIN conditions",
        ),
    ];
    let engine = SqlEngine::new();
    let mut setup_session = engine.connect();
    for statement in SETUP {
        setup_session.simple_query(statement).await.expect("setup");
    }
    for (sql, message) in cases {
        let error = engine
            .connect()
            .simple_query(sql)
            .await
            .expect_err("misplaced grouping call");
        assert!(error.code == "42803", "{sql}");
        assert!(error.message == message, "{sql}");
    }
}
