//! Q2: the `OVER` suffix, the `WINDOW` clause, and frame syntax.

use assert2::assert;
use crabka_pgparser::{
    ast::{
        Expr, FrameBound, FrameExclusion, FrameMode, FuncArgs, NamedWindow, OrderItem, QueryBody,
        SelectItem, SelectStmt, SetExpr, Statement, WindowCall, WindowFrame, WindowRef, WindowSpec,
        window_placeholder,
    },
    parse,
};

fn select(sql: &str) -> SelectStmt {
    let mut statements = parse(sql).unwrap_or_else(|e| panic!("parse `{sql}`: {e}"));
    let Some(Statement::Query(query)) = statements.pop() else {
        panic!("expected a query statement from `{sql}`");
    };
    let SetExpr::Query(QueryBody::Select(select)) = query.body else {
        panic!("expected a plain SELECT body from `{sql}`");
    };
    let mut select = *select;
    select.order_by = query.order_by;
    select
}

fn column(name: &str) -> Expr {
    Expr::Column {
        table: None,
        name: name.to_string(),
    }
}

fn ascending(name: &str) -> OrderItem {
    OrderItem {
        expr: column(name),
        asc: true,
        nulls_first: false,
    }
}

fn only_call(sql: &str) -> WindowCall {
    let mut select = select(sql);
    assert!(select.window_calls.len() == 1, "{sql}");
    select.window_calls.pop().expect("one call")
}

fn only_spec(sql: &str) -> WindowSpec {
    match only_call(sql).over {
        WindowRef::Spec(spec) => *spec,
        WindowRef::Named(name) => panic!("expected an inline spec, got the name `{name}`"),
    }
}

fn frame(sql: &str) -> WindowFrame {
    only_spec(sql).frame.expect("a frame clause")
}

#[test]
fn over_lifts_the_call_out_and_leaves_a_placeholder_behind() {
    let select = select("SELECT rank() OVER (PARTITION BY a ORDER BY b) FROM t");
    assert!(
        select.projection
            == vec![SelectItem::Expr {
                expr: window_placeholder(0, "rank"),
                alias: None,
            }]
    );
    assert!(
        select.window_calls
            == vec![WindowCall {
                name: "rank".into(),
                distinct: false,
                args: FuncArgs::Exprs(Vec::new()),
                filter: None,
                over: WindowRef::Spec(Box::new(WindowSpec {
                    base: None,
                    partition_by: vec![column("a")],
                    order_by: vec![ascending("b")],
                    frame: None,
                })),
            }]
    );
}

#[test]
fn each_call_takes_the_next_placeholder_index() {
    let select = select("SELECT row_number() OVER (), sum(x) OVER (ORDER BY a) FROM t");
    assert!(
        select.projection
            == vec![
                SelectItem::Expr {
                    expr: window_placeholder(0, "row_number"),
                    alias: None,
                },
                SelectItem::Expr {
                    expr: window_placeholder(1, "sum"),
                    alias: None,
                },
            ]
    );
    assert!(select.window_calls.len() == 2);
}

#[test]
fn over_accepts_a_window_name_and_the_window_clause_defines_it() {
    let select = select("SELECT rank() OVER w FROM t WINDOW w AS (ORDER BY a)");
    assert!(select.window_calls[0].over == WindowRef::Named("w".into()));
    assert!(
        select.windows
            == vec![NamedWindow {
                name: "w".into(),
                spec: WindowSpec {
                    base: None,
                    partition_by: Vec::new(),
                    order_by: vec![ascending("a")],
                    frame: None,
                },
            }]
    );
}

#[test]
fn a_window_specification_may_name_the_window_it_extends() {
    let select = select("SELECT rank() OVER (w ORDER BY b) FROM t WINDOW w AS (PARTITION BY a)");
    assert!(
        select.window_calls[0].over
            == WindowRef::Spec(Box::new(WindowSpec {
                base: Some("w".into()),
                partition_by: Vec::new(),
                order_by: vec![ascending("b")],
                frame: None,
            }))
    );
}

#[test]
fn window_order_by_carries_direction_and_null_placement() {
    let spec = only_spec("SELECT rank() OVER (ORDER BY a DESC, b ASC NULLS FIRST, c) FROM t");
    assert!(
        spec.order_by
            == vec![
                OrderItem {
                    expr: column("a"),
                    asc: false,
                    // PostgreSQL's default for DESC.
                    nulls_first: true,
                },
                OrderItem {
                    expr: column("b"),
                    asc: true,
                    nulls_first: true,
                },
                ascending("c"),
            ]
    );
}

#[test]
fn every_frame_mode_bound_and_exclusion_parses() {
    let cases: Vec<(&str, WindowFrame)> = vec![
        (
            "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            WindowFrame {
                mode: FrameMode::Rows,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
                exclusion: FrameExclusion::NoOthers,
            },
        ),
        (
            "RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING",
            WindowFrame {
                mode: FrameMode::Range,
                start: FrameBound::Preceding(Expr::IntLiteral("1".into())),
                end: FrameBound::Following(Expr::IntLiteral("2".into())),
                exclusion: FrameExclusion::NoOthers,
            },
        ),
        (
            "GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING EXCLUDE TIES",
            WindowFrame {
                mode: FrameMode::Groups,
                start: FrameBound::CurrentRow,
                end: FrameBound::Following(Expr::IntLiteral("1".into())),
                exclusion: FrameExclusion::Ties,
            },
        ),
        // The single-bound form expands to BETWEEN <bound> AND CURRENT ROW.
        (
            "ROWS UNBOUNDED PRECEDING",
            WindowFrame {
                mode: FrameMode::Rows,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
                exclusion: FrameExclusion::NoOthers,
            },
        ),
        (
            "ROWS 3 PRECEDING EXCLUDE CURRENT ROW",
            WindowFrame {
                mode: FrameMode::Rows,
                start: FrameBound::Preceding(Expr::IntLiteral("3".into())),
                end: FrameBound::CurrentRow,
                exclusion: FrameExclusion::CurrentRow,
            },
        ),
        (
            "ROWS CURRENT ROW EXCLUDE GROUP",
            WindowFrame {
                mode: FrameMode::Rows,
                start: FrameBound::CurrentRow,
                end: FrameBound::CurrentRow,
                exclusion: FrameExclusion::Group,
            },
        ),
        (
            "RANGE UNBOUNDED PRECEDING EXCLUDE NO OTHERS",
            WindowFrame {
                mode: FrameMode::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
                exclusion: FrameExclusion::NoOthers,
            },
        ),
    ];
    for (clause, expected) in cases {
        let sql = format!("SELECT count(*) OVER (ORDER BY a {clause}) FROM t");
        assert!(frame(&sql) == expected, "{clause}");
    }
}

#[test]
fn a_frame_offset_expression_does_not_swallow_the_between_and() {
    assert!(
        frame(
            "SELECT count(*) OVER (ORDER BY a ROWS BETWEEN 1 + 1 PRECEDING AND CURRENT ROW) FROM t"
        ) == WindowFrame {
            mode: FrameMode::Rows,
            start: FrameBound::Preceding(Expr::Binary {
                op: crabka_pgparser::ast::BinaryOp::Add,
                left: Box::new(Expr::IntLiteral("1".into())),
                right: Box::new(Expr::IntLiteral("1".into())),
            }),
            end: FrameBound::CurrentRow,
            exclusion: FrameExclusion::NoOthers,
        }
    );
}

#[test]
fn filter_attaches_to_the_window_call() {
    let call = only_call("SELECT count(*) FILTER (WHERE a > 1) OVER () FROM t");
    assert!(
        call == WindowCall {
            name: "count".into(),
            distinct: false,
            args: FuncArgs::Star,
            filter: Some(Expr::Binary {
                op: crabka_pgparser::ast::BinaryOp::Gt,
                left: Box::new(column("a")),
                right: Box::new(Expr::IntLiteral("1".into())),
            }),
            over: WindowRef::Spec(Box::default()),
        }
    );
}

#[test]
fn distinct_survives_onto_the_window_call_for_the_executor_to_refuse() {
    let call = only_call("SELECT sum(DISTINCT a) OVER () FROM t");
    assert!(call.distinct);
}

#[test]
fn a_frame_clause_word_is_not_mistaken_for_an_existing_window_name() {
    for mode in ["ROWS", "RANGE", "GROUPS"] {
        let sql = format!("SELECT count(*) OVER ({mode} UNBOUNDED PRECEDING) FROM t");
        assert!(only_spec(&sql).base.is_none(), "{mode}");
    }
}

#[test]
fn a_subquery_owns_the_window_calls_written_inside_it() {
    let outer = select(
        "SELECT (SELECT max(r) FROM (SELECT rank() OVER (ORDER BY b) AS r FROM u) i) \
         FROM t ORDER BY row_number() OVER (ORDER BY a)",
    );
    // Only the ORDER BY call belongs to the outer SELECT.
    assert!(outer.window_calls.len() == 1);
    assert!(outer.window_calls[0].name == "row_number");
    assert!(outer.order_by[0].expr == window_placeholder(0, "row_number"));
}

#[test]
fn window_syntax_errors_are_reported() {
    for sql in [
        "SELECT count(*) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING) FROM t",
        "SELECT count(*) OVER (ORDER BY a ROWS 1) FROM t",
        "SELECT count(*) OVER (ORDER BY a EXCLUDE) FROM t",
        "SELECT count(*) OVER FROM t",
        // `FILTER` needs the `WHERE` keyword inside its parentheses.
        "SELECT count(*) FILTER (a > 1) OVER () FROM t",
        "SELECT count(*) OVER () FROM t WINDOW w AS (",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }

    // `FILTER` WITHOUT `OVER` is the plain aggregate spelling and parses: the
    // call carries the predicate and the aggregate path applies it per row.
    for sql in [
        "SELECT count(*) FILTER (WHERE a > 1) FROM t",
        "SELECT a, count(*) FILTER (WHERE b > 1) FROM t GROUP BY a",
        "SELECT count(*) FILTER (WHERE a > 1) OVER () FROM t",
    ] {
        assert!(parse(sql).is_ok(), "{sql}");
    }
}

#[test]
fn a_window_call_outside_any_select_is_refused() {
    let error = parse("INSERT INTO t VALUES (row_number() OVER ())").expect_err("42P20");
    assert!(error.sqlstate() == "42P20");
}

#[test]
fn a_duplicate_window_name_is_refused_by_the_clause_itself() {
    let error = parse("SELECT count(*) OVER () FROM t WINDOW w AS (), w AS ()").expect_err("42P20");
    assert!(error.sqlstate() == "42P20");
}

#[test]
fn a_window_call_in_a_window_definition_is_refused() {
    for sql in [
        "SELECT count(*) OVER (ORDER BY rank() OVER ()) FROM t",
        "SELECT count(*) OVER (PARTITION BY row_number() OVER ()) FROM t",
        "SELECT rank() OVER (ORDER BY row_number() OVER ()) FROM t",
        "SELECT count(*) OVER (ORDER BY a ROWS rank() OVER () PRECEDING) FROM t",
        "SELECT count(*) OVER w FROM t WINDOW w AS (ORDER BY rank() OVER ())",
    ] {
        let error = parse(sql)
            .err()
            .unwrap_or_else(|| panic!("`{sql}` unexpectedly parsed"));
        assert!(error.sqlstate() == "42P20", "{sql}");
    }
}

#[test]
fn a_subquery_tail_owns_the_window_calls_written_in_it() {
    // The tail of a PARENTHESIZED query belongs to the SELECT inside the
    // parentheses, exactly as a top-level tail belongs to the top-level SELECT.
    for sql in [
        "SELECT a FROM (SELECT a FROM t ORDER BY rank() OVER (ORDER BY b) LIMIT 2) q",
        "SELECT * FROM t WHERE a IN (SELECT a FROM t ORDER BY rank() OVER (ORDER BY b) LIMIT 2)",
    ] {
        let outer = select(sql);
        assert!(outer.window_calls.is_empty(), "{sql}");
    }
    // ... and the top-level form still registers on the top-level SELECT.
    let top = select("SELECT a FROM t ORDER BY rank() OVER (ORDER BY b) LIMIT 2");
    assert!(top.window_calls.len() == 1);
}

#[test]
fn words_postgres_bars_from_a_bare_column_label_are_not_aliases() {
    // `pg_get_keywords()` marks each of these `barelabel = false`.
    for word in ["window", "over", "filter"] {
        let sql = format!("SELECT a {word} FROM t");
        assert!(parse(&sql).is_err(), "{sql}");
        // `AS` takes any ColLabel, so the explicit form is still accepted.
        let sql = format!("SELECT a AS {word} FROM t");
        assert!(parse(&sql).is_ok(), "{sql}");
    }
}

#[test]
fn a_from_item_alias_cannot_be_spelled_with_a_reserved_word() {
    // `alias_clause` takes a `ColId`, which excludes all three however the alias
    // is introduced — the `AS` form is a syntax error just like the bare one.
    for word in ["window", "fetch", "tablesample"] {
        for sql in [
            format!("SELECT * FROM t AS {word}"),
            format!("SELECT * FROM t {word}"),
        ] {
            assert!(parse(&sql).is_err(), "{sql}");
        }
    }
    // `over` and `filter` are unreserved, so they ARE valid FROM-item aliases.
    for word in ["over", "filter"] {
        for sql in [
            format!("SELECT * FROM t AS {word}"),
            format!("SELECT * FROM t {word}"),
        ] {
            assert!(parse(&sql).is_ok(), "{sql}");
        }
    }
}
