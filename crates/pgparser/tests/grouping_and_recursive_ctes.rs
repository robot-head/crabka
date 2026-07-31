//! Q3 grammar: `GROUP BY` grouping sets and the recursive-CTE `SEARCH`/`CYCLE`
//! clauses.

use assert2::assert;
use crabka_pgparser::{
    ast::{
        Cte, CteCycle, CteSearch, Expr, GroupItem, GroupingClause, QueryBody, SelectStmt, SetExpr,
        Statement,
    },
    parse,
};

fn select(sql: &str) -> SelectStmt {
    let Statement::Query(query) = parse(sql).expect("parse").pop().expect("one statement") else {
        panic!("expected a query: {sql}")
    };
    match query.body {
        SetExpr::Query(QueryBody::Select(select)) => *select,
        _ => panic!("expected a plain SELECT: {sql}"),
    }
}

fn ctes(sql: &str) -> Vec<Cte> {
    let Statement::Query(query) = parse(sql).expect("parse").pop().expect("one statement") else {
        panic!("expected a query: {sql}")
    };
    query.with.expect("a WITH clause").ctes
}

fn column(name: &str) -> Expr {
    Expr::Column {
        table: None,
        name: name.to_string(),
    }
}

fn sqlstate(sql: &str) -> &'static str {
    parse(sql).expect_err("expected a parse error").sqlstate()
}

#[test]
fn plain_group_by_has_no_set_structure() {
    let s = select("SELECT a, count(*) FROM t GROUP BY a, b");
    assert!(s.group_by == vec![column("a"), column("b")]);
    assert!(s.grouping == None);
}

#[test]
fn a_bare_tuple_at_the_top_of_the_list_is_flattened() {
    // PostgreSQL treats `GROUP BY (a, b)` as `GROUP BY a, b`, not as grouping by
    // one row value.
    let s = select("SELECT a FROM t GROUP BY (a, b)");
    assert!(s.group_by == vec![column("a"), column("b")]);
    assert!(s.grouping == None);
}

#[test]
fn grouping_set_constructs_parse_to_their_items() {
    let cases: Vec<(&str, Vec<Expr>, GroupingClause)> = vec![
        (
            "SELECT a FROM t GROUP BY ROLLUP(a, b)",
            vec![column("a"), column("b")],
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::Rollup(vec![
                    GroupItem::Expr(0),
                    GroupItem::Expr(1),
                ])],
            },
        ),
        (
            "SELECT a FROM t GROUP BY CUBE(a, b)",
            vec![column("a"), column("b")],
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::Cube(vec![
                    GroupItem::Expr(0),
                    GroupItem::Expr(1),
                ])],
            },
        ),
        (
            "SELECT a FROM t GROUP BY GROUPING SETS ((a, b), (a), ())",
            vec![column("a"), column("b")],
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::GroupingSets(vec![
                    GroupItem::Composite(vec![0, 1]),
                    GroupItem::Expr(0),
                    GroupItem::Empty,
                ])],
            },
        ),
        (
            "SELECT a FROM t GROUP BY ()",
            Vec::new(),
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::Empty],
            },
        ),
        (
            // Repeating an expression reuses its index, so a grouping set stays a
            // set of grouping columns.
            "SELECT a FROM t GROUP BY ROLLUP(a), CUBE(a)",
            vec![column("a")],
            GroupingClause {
                distinct: false,
                items: vec![
                    GroupItem::Rollup(vec![GroupItem::Expr(0)]),
                    GroupItem::Cube(vec![GroupItem::Expr(0)]),
                ],
            },
        ),
        (
            "SELECT a FROM t GROUP BY ROLLUP((a, b), c)",
            vec![column("a"), column("b"), column("c")],
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::Rollup(vec![
                    GroupItem::Composite(vec![0, 1]),
                    GroupItem::Expr(2),
                ])],
            },
        ),
        (
            "SELECT a FROM t GROUP BY GROUPING SETS (ROLLUP(a), b)",
            vec![column("a"), column("b")],
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::GroupingSets(vec![
                    GroupItem::Rollup(vec![GroupItem::Expr(0)]),
                    GroupItem::Expr(1),
                ])],
            },
        ),
        (
            "SELECT a FROM t GROUP BY DISTINCT ROLLUP(a)",
            vec![column("a")],
            GroupingClause {
                distinct: true,
                items: vec![GroupItem::Rollup(vec![GroupItem::Expr(0)])],
            },
        ),
        (
            "SELECT a FROM t GROUP BY ALL ROLLUP(a)",
            vec![column("a")],
            GroupingClause {
                distinct: false,
                items: vec![GroupItem::Rollup(vec![GroupItem::Expr(0)])],
            },
        ),
        (
            // `DISTINCT` alone still needs the clause: it deduplicates the sets.
            "SELECT a FROM t GROUP BY DISTINCT a, a",
            vec![column("a")],
            GroupingClause {
                distinct: true,
                items: vec![GroupItem::Expr(0), GroupItem::Expr(0)],
            },
        ),
    ];
    for (sql, exprs, clause) in cases {
        let s = select(sql);
        assert!(s.group_by == exprs, "{sql}");
        assert!(s.grouping == Some(clause), "{sql}");
    }
}

#[test]
fn rollup_and_cube_stay_usable_as_ordinary_names() {
    // They are unreserved in PostgreSQL, so only the following `(` makes them
    // grouping constructs.
    let s = select("SELECT rollup FROM t GROUP BY rollup, cube");
    assert!(s.group_by == vec![column("rollup"), column("cube")]);
    assert!(s.grouping == None);
}

#[test]
fn grouping_set_size_limits_are_parse_errors() {
    assert!(sqlstate("SELECT 1 FROM t GROUP BY CUBE(a,b,c,d,e,f,g,h,i,j,k,l,m)") == "54011");
    assert!(
        sqlstate(
            "SELECT 1 FROM t GROUP BY CUBE(a,b,c,d,e,f,g,h,i,j,k,l), CUBE(a,b,c,d,e,f,g,h,i,j,k,l)"
        ) == "54001"
    );
    // Twelve elements is the limit, not an error.
    assert!(parse("SELECT 1 FROM t GROUP BY CUBE(a,b,c,d,e,f,g,h,i,j,k,l)").is_ok());
    // ROLLUP's expansion is linear, so it has no element cap.
    assert!(parse("SELECT 1 FROM t GROUP BY ROLLUP(a,b,c,d,e,f,g,h,i,j,k,l,m)").is_ok());
}

#[test]
fn search_and_cycle_clauses_parse() {
    let parsed = ctes(
        "WITH RECURSIVE t AS (SELECT 1 AS n) \
         SEARCH BREADTH FIRST BY n, m SET seq \
         CYCLE n SET is_cycle TO 'Y' DEFAULT 'N' USING path \
         SELECT n FROM t",
    );
    assert!(
        parsed[0].search
            == Some(CteSearch {
                depth_first: false,
                by: vec!["n".into(), "m".into()],
                set: "seq".into(),
            })
    );
    assert!(
        parsed[0].cycle
            == Some(CteCycle {
                by: vec!["n".into()],
                set: "is_cycle".into(),
                mark_values: Some((
                    Expr::StringLiteral("Y".into()),
                    Expr::StringLiteral("N".into())
                )),
                using: "path".into(),
            })
    );
}

#[test]
fn depth_first_search_and_default_cycle_marks() {
    let parsed = ctes(
        "WITH RECURSIVE t AS (SELECT 1 AS n) \
         SEARCH DEPTH FIRST BY n SET seq CYCLE n SET is_cycle USING path SELECT n FROM t",
    );
    let search = parsed[0].search.as_ref().expect("a SEARCH clause");
    assert!(search.depth_first);
    let cycle = parsed[0].cycle.as_ref().expect("a CYCLE clause");
    assert!(cycle.mark_values == None);
}

#[test]
fn materialization_hints_are_recorded_per_item() {
    let parsed = ctes(
        "WITH a AS MATERIALIZED (SELECT 1), b AS NOT MATERIALIZED (SELECT 2), c AS (SELECT 3) \
         SELECT 1",
    );
    let hints: Vec<Option<bool>> = parsed.iter().map(|cte| cte.materialized).collect();
    assert!(hints == vec![Some(true), Some(false), None]);
}

#[test]
fn a_cte_without_search_or_cycle_records_neither() {
    let parsed = ctes("WITH RECURSIVE t AS (SELECT 1 AS n) SELECT n FROM t");
    assert!(parsed[0].search == None);
    assert!(parsed[0].cycle == None);
}
