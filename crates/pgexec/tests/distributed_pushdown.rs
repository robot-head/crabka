use std::sync::{Arc, Mutex};

use crabka_pgcatalog::{Column, Table};
use crabka_pgexec::{
    ColumnPredicate, PartialAggregateFunction, PartialAggregateSpec, PredicateOp,
    PredicatePushdown, ProjectionPushdown, RangeScanner, ScanRequest, ScannedRow, SqlEngine,
    TopKColumn, TopKSpec,
    plan_dist::{plan_scan, strict_predicate_for_filter},
    scanner::{
        LocalRangeScanner, apply_executable_scan_pushdown, apply_scan_pushdown,
        finalize_partial_aggregate_rows,
    },
};
use crabka_pgparser::ast::{BinaryOp, Expr, SelectItem};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

fn table() -> Table {
    Table {
        id: 42,
        name: "items".to_string(),
        columns: vec![
            Column::new("id", ColumnType::Int4),
            Column::new("name", ColumnType::Text),
        ],
        sharded: true,
        sharding: None,
        foreign: None,
    }
}

fn expr(sql: &str) -> Expr {
    crabka_pgparser::parser::parse_expr_for_test(sql).expect("predicate parses")
}

#[test]
fn equality_and_range_predicates_match_full_scan_filtering() {
    let table = table();
    let predicate =
        strict_predicate_for_filter(&table, Some(&expr("id >= 2 AND id < 5 AND name = 'keep'")))
            .expect("supported predicate");
    let rows = vec![
        ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(1), Datum::Text("keep".into())],
        },
        ScannedRow {
            rowid: 2,
            xmin: 1,
            row: vec![Datum::Int4(2), Datum::Text("keep".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 1,
            row: vec![Datum::Int4(4), Datum::Text("drop".into())],
        },
        ScannedRow {
            rowid: 4,
            xmin: 1,
            row: vec![Datum::Int4(5), Datum::Text("keep".into())],
        },
    ];

    let pushed = apply_scan_pushdown(rows.clone(), &predicate, &ProjectionPushdown::All)
        .expect("pushdown applies");
    let full_scan_filtered = rows
        .into_iter()
        .filter(|row| matches!(row.row.as_slice(), [Datum::Int4(id), Datum::Text(name)] if (2..5).contains(id) && name == "keep"))
        .collect::<Vec<_>>();

    assert_eq!(pushed, full_scan_filtered);
}

#[test]
fn projection_pushdown_returns_requested_columns_in_order() {
    let rows = vec![ScannedRow {
        rowid: 7,
        xmin: 3,
        row: vec![Datum::Int4(9), Datum::Text("nine".into())],
    }];

    let pushed = apply_scan_pushdown(
        rows,
        &PredicatePushdown::FullScan,
        &ProjectionPushdown::Columns(vec![1, 0]),
    )
    .expect("projection applies");

    assert_eq!(
        pushed[0].row,
        vec![Datum::Text("nine".into()), Datum::Int4(9)]
    );
}

#[test]
fn partial_count_pushdown_matches_full_scan_count_after_predicate() {
    let rows = vec![
        ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(1), Datum::Text("drop".into())],
        },
        ScannedRow {
            rowid: 2,
            xmin: 1,
            row: vec![Datum::Int4(2), Datum::Text("keep".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 1,
            row: vec![Datum::Int4(3), Datum::Text("keep".into())],
        },
    ];
    let predicate = PredicatePushdown::Conjunctive(vec![ColumnPredicate {
        column: 1,
        op: PredicateOp::Eq,
        value: Datum::Text("keep".into()),
    }]);

    let pushed = apply_executable_scan_pushdown(
        rows,
        &predicate,
        &ProjectionPushdown::All,
        Some(&PartialAggregateSpec {
            function: PartialAggregateFunction::Count,
            column: None,
        }),
        None,
    )
    .expect("partial count applies");

    assert_eq!(pushed[0].row, vec![Datum::Int8(2)]);
}

#[test]
fn top_k_pushdown_matches_full_scan_order_limit_with_deterministic_ties() {
    let rows = vec![
        ScannedRow {
            rowid: 4,
            xmin: 1,
            row: vec![Datum::Int4(10), Datum::Text("d".into())],
        },
        ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(20), Datum::Text("a".into())],
        },
        ScannedRow {
            rowid: 2,
            xmin: 1,
            row: vec![Datum::Int4(20), Datum::Text("b".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 1,
            row: vec![Datum::Int4(30), Datum::Text("c".into())],
        },
    ];

    let pushed = apply_executable_scan_pushdown(
        rows,
        &PredicatePushdown::FullScan,
        &ProjectionPushdown::All,
        None,
        Some(&TopKSpec {
            order_by: vec![TopKColumn {
                column: 0,
                asc: false,
            }],
            limit: 3,
        }),
    )
    .expect("top-k applies");

    assert_eq!(
        pushed.iter().map(|row| row.rowid).collect::<Vec<_>>(),
        vec![3, 1, 2]
    );
}

#[test]
fn top_k_pushdown_supports_ascending_order() {
    let rows = vec![
        ScannedRow {
            rowid: 4,
            xmin: 1,
            row: vec![Datum::Int4(10), Datum::Text("d".into())],
        },
        ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(20), Datum::Text("a".into())],
        },
        ScannedRow {
            rowid: 2,
            xmin: 1,
            row: vec![Datum::Int4(20), Datum::Text("b".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 1,
            row: vec![Datum::Int4(30), Datum::Text("c".into())],
        },
    ];

    let pushed = apply_executable_scan_pushdown(
        rows,
        &PredicatePushdown::FullScan,
        &ProjectionPushdown::All,
        None,
        Some(&TopKSpec {
            order_by: vec![TopKColumn {
                column: 0,
                asc: true,
            }],
            limit: 3,
        }),
    )
    .expect("ascending top-k applies");

    assert_eq!(
        pushed.iter().map(|row| row.rowid).collect::<Vec<_>>(),
        vec![4, 1, 2]
    );
}

#[test]
fn top_k_pushdown_orders_multiple_keys_before_deterministic_identity() {
    let mut rows = vec![
        ScannedRow {
            rowid: 9,
            xmin: 2,
            row: vec![Datum::Int4(1), Datum::Text("a".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 4,
            row: vec![Datum::Int4(1), Datum::Text("b".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 1,
            row: vec![Datum::Int4(1), Datum::Text("b".into())],
        },
        ScannedRow {
            rowid: 2,
            xmin: 1,
            row: vec![Datum::Int4(2), Datum::Text("z".into())],
        },
    ];

    crabka_pgexec::scanner::apply_top_k_pushdown(
        &mut rows,
        &TopKSpec {
            order_by: vec![
                TopKColumn {
                    column: 0,
                    asc: true,
                },
                TopKColumn {
                    column: 1,
                    asc: false,
                },
            ],
            limit: 3,
        },
    )
    .expect("multi-key top-k applies");

    assert_eq!(
        rows.iter()
            .map(|row| (row.rowid, row.xmin))
            .collect::<Vec<_>>(),
        vec![(3, 1), (3, 4), (9, 2)]
    );
}

#[test]
fn top_k_pushdown_merges_uneven_range_local_results_lexicographically() {
    let spec = TopKSpec {
        order_by: vec![
            TopKColumn {
                column: 0,
                asc: true,
            },
            TopKColumn {
                column: 1,
                asc: false,
            },
        ],
        limit: 3,
    };
    let ranges = [
        vec![
            ScannedRow {
                rowid: 8,
                xmin: 1,
                row: vec![Datum::Int4(1), Datum::Text("z".into())],
            },
            ScannedRow {
                rowid: 1,
                xmin: 1,
                row: vec![Datum::Int4(1), Datum::Text("a".into())],
            },
            ScannedRow {
                rowid: 5,
                xmin: 1,
                row: vec![Datum::Int4(3), Datum::Text("x".into())],
            },
            ScannedRow {
                rowid: 7,
                xmin: 1,
                row: vec![Datum::Int4(4), Datum::Text("x".into())],
            },
        ],
        vec![
            ScannedRow {
                rowid: 3,
                xmin: 2,
                row: vec![Datum::Int4(1), Datum::Text("z".into())],
            },
            ScannedRow {
                rowid: 2,
                xmin: 1,
                row: vec![Datum::Int4(2), Datum::Text("q".into())],
            },
        ],
    ];

    let mut merged = ranges
        .into_iter()
        .flat_map(|mut range| {
            crabka_pgexec::scanner::apply_top_k_pushdown(&mut range, &spec)
                .expect("range-local top-k applies");
            range
        })
        .collect::<Vec<_>>();
    crabka_pgexec::scanner::apply_top_k_pushdown(&mut merged, &spec)
        .expect("global top-k merge applies");

    assert_eq!(
        merged
            .iter()
            .map(|row| (row.rowid, row.xmin))
            .collect::<Vec<_>>(),
        vec![(3, 2), (8, 1), (1, 1)]
    );
}

#[test]
fn top_k_pushdown_rejects_projection_pushdown() {
    let error = apply_executable_scan_pushdown(
        vec![ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(10), Datum::Text("ten".into())],
        }],
        &PredicatePushdown::FullScan,
        &ProjectionPushdown::Columns(vec![1]),
        None,
        Some(&TopKSpec {
            order_by: vec![TopKColumn {
                column: 0,
                asc: false,
            }],
            limit: 1,
        }),
    )
    .expect_err("top-k over projected rows is ambiguous");

    assert!(
        error
            .into_pg()
            .message
            .contains("top-k pushdown cannot be combined with projection pushdown")
    );
}

#[test]
fn top_k_pushdown_rejects_null_order_key() {
    let error = apply_executable_scan_pushdown(
        vec![ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Null],
        }],
        &PredicatePushdown::FullScan,
        &ProjectionPushdown::All,
        None,
        Some(&TopKSpec {
            order_by: vec![TopKColumn {
                column: 0,
                asc: true,
            }],
            limit: 1,
        }),
    )
    .expect_err("null order keys are not executable top-k pushdown");

    assert!(
        error
            .into_pg()
            .message
            .contains("top-k pushdown supports only non-null int4/int8/text order keys")
    );
}

#[test]
fn top_k_pushdown_rejects_missing_order_key() {
    let error = apply_executable_scan_pushdown(
        vec![ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(10)],
        }],
        &PredicatePushdown::FullScan,
        &ProjectionPushdown::All,
        None,
        Some(&TopKSpec {
            order_by: vec![TopKColumn {
                column: 1,
                asc: true,
            }],
            limit: 1,
        }),
    )
    .expect_err("missing order keys are not executable top-k pushdown");

    assert!(
        error
            .into_pg()
            .message
            .contains("top-k pushdown column 1 is outside the scanned row")
    );
}

#[test]
fn partial_sum_count_column_min_max_preserve_null_semantics() {
    let rows = vec![
        ScannedRow {
            rowid: 1,
            xmin: 1,
            row: vec![Datum::Int4(10), Datum::Text("drop".into())],
        },
        ScannedRow {
            rowid: 2,
            xmin: 1,
            row: vec![Datum::Null, Datum::Text("keep".into())],
        },
        ScannedRow {
            rowid: 3,
            xmin: 1,
            row: vec![Datum::Int4(7), Datum::Text("keep".into())],
        },
        ScannedRow {
            rowid: 4,
            xmin: 1,
            row: vec![Datum::Int4(3), Datum::Text("keep".into())],
        },
    ];
    let predicate = PredicatePushdown::Conjunctive(vec![ColumnPredicate {
        column: 1,
        op: PredicateOp::Eq,
        value: Datum::Text("keep".into()),
    }]);

    let cases = [
        (PartialAggregateFunction::Count, vec![Datum::Int8(2)]),
        (PartialAggregateFunction::Sum, vec![Datum::Int8(10)]),
        (PartialAggregateFunction::Min, vec![Datum::Int4(3)]),
        (PartialAggregateFunction::Max, vec![Datum::Int4(7)]),
    ];

    for (function, expected) in cases {
        let pushed = apply_executable_scan_pushdown(
            rows.clone(),
            &predicate,
            &ProjectionPushdown::All,
            Some(&PartialAggregateSpec {
                function,
                column: Some(0),
            }),
            None,
        )
        .expect("partial aggregate applies");

        assert_eq!(pushed[0].row, expected);
    }
}

#[test]
fn partial_sum_min_max_over_empty_input_return_null() {
    for function in [
        PartialAggregateFunction::Sum,
        PartialAggregateFunction::Min,
        PartialAggregateFunction::Max,
    ] {
        let pushed = apply_executable_scan_pushdown(
            Vec::new(),
            &PredicatePushdown::FullScan,
            &ProjectionPushdown::All,
            Some(&PartialAggregateSpec {
                function,
                column: Some(0),
            }),
            None,
        )
        .expect("empty partial aggregate applies");

        assert_eq!(pushed[0].row, vec![Datum::Null]);
    }
}

#[test]
fn partial_avg_merges_sum_count_across_uneven_ranges_and_preserves_null_semantics() {
    let spec = PartialAggregateSpec {
        function: PartialAggregateFunction::AvgParts,
        column: Some(0),
    };
    let predicate = PredicatePushdown::Conjunctive(vec![ColumnPredicate {
        column: 1,
        op: PredicateOp::Eq,
        value: Datum::Text("keep".into()),
    }]);
    let ranges = vec![
        vec![
            ScannedRow {
                rowid: 1,
                xmin: 1,
                row: vec![Datum::Int4(10), Datum::Text("keep".into())],
            },
            ScannedRow {
                rowid: 2,
                xmin: 1,
                row: vec![Datum::Null, Datum::Text("keep".into())],
            },
        ],
        vec![
            ScannedRow {
                rowid: 3,
                xmin: 1,
                row: vec![Datum::Int4(7), Datum::Text("keep".into())],
            },
            ScannedRow {
                rowid: 4,
                xmin: 1,
                row: vec![Datum::Int4(100), Datum::Text("keep".into())],
            },
            ScannedRow {
                rowid: 5,
                xmin: 1,
                row: vec![Datum::Int4(1), Datum::Text("drop".into())],
            },
        ],
        Vec::new(),
    ];

    let partials = ranges
        .into_iter()
        .flat_map(|rows| {
            apply_executable_scan_pushdown(
                rows,
                &predicate,
                &ProjectionPushdown::All,
                Some(&spec),
                None,
            )
            .expect("range AVG parts apply")
        })
        .collect();
    let pushed = finalize_partial_aggregate_rows(partials, &spec).expect("AVG parts finalize");

    let expected = crabka_pgtypes::ops::div(
        &Datum::Numeric(bigdecimal::BigDecimal::from(117)),
        &Datum::Int8(3),
    )
    .expect("expected exact numeric average");
    assert_eq!(pushed[0].row, vec![expected]);

    let empty = finalize_partial_aggregate_rows(
        apply_executable_scan_pushdown(
            Vec::new(),
            &PredicatePushdown::FullScan,
            &ProjectionPushdown::All,
            Some(&spec),
            None,
        )
        .expect("empty range AVG parts apply"),
        &spec,
    )
    .expect("empty AVG parts finalize");
    assert_eq!(empty[0].row, vec![Datum::Null]);
}

#[test]
fn planner_emits_deterministic_predicate_projection_and_partial_count_shape() {
    let table = table();
    let projection = vec![SelectItem::Expr {
        expr: Expr::Column {
            table: None,
            name: "name".to_string(),
        },
        alias: None,
    }];

    let plan = plan_scan(&table, Some(&expr("id = 3")), &projection);

    assert_eq!(
        plan.predicate,
        PredicatePushdown::Conjunctive(vec![ColumnPredicate {
            column: 0,
            op: PredicateOp::Eq,
            value: Datum::Int4(3),
        }])
    );
    assert_eq!(plan.projection, ProjectionPushdown::Columns(vec![1]));
    assert_eq!(plan.partial_aggregate, None);
}

#[test]
fn unsupported_predicate_fails_clearly_for_strict_pushdown() {
    let table = table();

    let error = strict_predicate_for_filter(&table, Some(&expr("id = 1 OR name = 'x'")))
        .expect_err("OR is not supported by strict predicate pushdown");

    assert!(
        error
            .into_pg()
            .message
            .contains("supports only column literal equality/range conjuncts")
    );
}

#[test]
fn strict_predicate_rejects_const_types_the_scanner_cannot_execute() {
    let table = Table {
        id: 99,
        name: "measurements".to_string(),
        columns: vec![Column::new("reading", ColumnType::Float8)],
        sharded: true,
        sharding: None,
        foreign: None,
    };
    let filter = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Column {
            table: None,
            name: "reading".to_string(),
        }),
        right: Box::new(Expr::Const {
            value: Datum::Float8(1.5),
            ty: ColumnType::Float8,
        }),
    };

    let error = strict_predicate_for_filter(&table, Some(&filter))
        .expect_err("float8 predicates are not executable scanner predicates");

    assert!(
        error
            .into_pg()
            .message
            .contains("supports only column literal equality/range conjuncts")
    );
}

#[derive(Debug, Clone, Default)]
struct RecordedScan {
    table: String,
    predicate: PredicatePushdown,
    projection: ProjectionPushdown,
    partial_aggregate: Option<PartialAggregateSpec>,
    top_k: Option<TopKSpec>,
}

#[derive(Debug, Default)]
struct RecordingScanner {
    scans: Mutex<Vec<RecordedScan>>,
}

impl RecordingScanner {
    fn scans(&self) -> Vec<RecordedScan> {
        self.scans.lock().expect("scan log").clone()
    }
}

impl RangeScanner for RecordingScanner {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, crabka_pgexec::ExecError> {
        self.scans.lock().expect("scan log").push(RecordedScan {
            table: request.table.name.clone(),
            predicate: request.predicate.clone(),
            projection: request.projection.clone(),
            partial_aggregate: request.partial_aggregate.clone(),
            top_k: request.top_k.clone(),
        });
        LocalRangeScanner.scan(request)
    }
}

#[derive(Debug, Default)]
struct RejectExecutableScanner;

impl RangeScanner for RejectExecutableScanner {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, crabka_pgexec::ExecError> {
        if request.partial_aggregate.is_some() {
            return Err(crabka_pgexec::ExecError::Unsupported(
                "injected partial aggregate pushdown failure".into(),
            ));
        }
        if request.top_k.is_some() {
            return Err(crabka_pgexec::ExecError::Unsupported(
                "injected top-k pushdown failure".into(),
            ));
        }
        LocalRangeScanner.scan(request)
    }
}

async fn query_cells(engine: SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    let mut session = engine.connect();
    let result = session
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .pop()
        .expect("one result");
    cells(result)
}

fn cells(result: QueryResult) -> Vec<Vec<Option<String>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| {
                        cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

async fn seed_sharded_items(engine: &SqlEngine) {
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE items (id int4 NOT NULL, name text NOT NULL) SHARDED")
        .await
        .expect("create sharded table");
    session
        .simple_query(
            "INSERT INTO items VALUES \
             (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')",
        )
        .await
        .expect("insert rows");
}

async fn seed_nullable_order_items(engine: &SqlEngine) {
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE nullable_items (id int4, name text) SHARDED")
        .await
        .expect("create nullable_items table");
    session
        .simple_query(
            "INSERT INTO nullable_items VALUES \
             (NULL, 'nil'), (2, 'two'), (1, 'one')",
        )
        .await
        .expect("insert nullable_items rows");
}

async fn seed_sharded_metrics(engine: &SqlEngine) {
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE metrics (id int4, amount int4, label text) SHARDED")
        .await
        .expect("create metrics table");
    session
        .simple_query(
            "INSERT INTO metrics VALUES \
             (1, 10, 'keep'), (2, NULL, 'keep'), (3, 7, 'keep'), \
             (4, 4, 'drop'), (5, NULL, 'drop')",
        )
        .await
        .expect("insert metrics rows");
}

async fn seed_sharded_float_metrics(engine: &SqlEngine) {
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE float_metrics (reading float8, amount int4) SHARDED")
        .await
        .expect("create float_metrics table");
    session
        .simple_query("INSERT INTO float_metrics VALUES (0.1::float8, 10), (0.2::float8, 20)")
        .await
        .expect("insert float_metrics rows");
}

async fn seed_sharded_exact_avg_values(engine: &SqlEngine) {
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE exact_avg_values (i8 int8, n numeric) SHARDED")
        .await
        .expect("create exact_avg_values table");
    session
        .simple_query(
            "INSERT INTO exact_avg_values VALUES \
             (10, 1.25::numeric), (NULL, NULL), (5, 2.75::numeric)",
        )
        .await
        .expect("insert exact AVG values");
}

#[tokio::test]
async fn sql_simple_aggregates_request_partial_pushdown_and_match_full_scan() {
    let cases = [
        (
            "SELECT count(amount) FROM metrics WHERE label = 'keep'",
            PartialAggregateFunction::Count,
            vec![vec![Some("2".into())]],
        ),
        (
            "SELECT sum(amount) FROM metrics WHERE label = 'keep'",
            PartialAggregateFunction::Sum,
            vec![vec![Some("17".into())]],
        ),
        (
            "SELECT min(amount) FROM metrics WHERE label = 'keep'",
            PartialAggregateFunction::Min,
            vec![vec![Some("7".into())]],
        ),
        (
            "SELECT max(amount) FROM metrics WHERE label = 'keep'",
            PartialAggregateFunction::Max,
            vec![vec![Some("10".into())]],
        ),
    ];

    for (sql, function, expected) in cases {
        let scanner = Arc::new(RecordingScanner::default());
        let mut pushed_engine = SqlEngine::new();
        pushed_engine.set_range_scanner(scanner.clone());
        seed_sharded_metrics(&pushed_engine).await;

        let pushed = query_cells(pushed_engine, sql).await;

        let full_engine = SqlEngine::new();
        seed_sharded_metrics(&full_engine).await;
        let full = query_cells(full_engine, sql).await;

        assert_eq!(pushed, full);
        assert_eq!(pushed, expected);
        assert!(scanner.scans().iter().any(|scan| {
            scan.partial_aggregate
                == Some(PartialAggregateSpec {
                    function,
                    column: Some(1),
                })
        }));
    }
}

#[tokio::test]
async fn sql_partial_sum_over_empty_input_matches_full_scan_null() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_metrics(&pushed_engine).await;

    let sql = "SELECT sum(amount) FROM metrics WHERE label = 'missing'";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_metrics(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert_eq!(pushed, vec![vec![None]]);
    assert!(scanner.scans().iter().any(|scan| {
        scan.partial_aggregate
            == Some(PartialAggregateSpec {
                function: PartialAggregateFunction::Sum,
                column: Some(1),
            })
    }));
}

#[tokio::test]
async fn sql_int4_avg_requests_sum_count_parts_and_matches_local_aggregate() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_metrics(&pushed_engine).await;

    let sql = "SELECT avg(amount) FROM metrics WHERE label = 'keep'";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_metrics(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert!(scanner.scans().iter().any(|scan| {
        scan.partial_aggregate
            == Some(PartialAggregateSpec {
                function: PartialAggregateFunction::AvgParts,
                column: Some(1),
            })
    }));
}

#[tokio::test]
async fn int8_and_numeric_avg_pushdown_match_local_numeric_results() {
    for (sql, column) in [
        ("SELECT avg(i8) FROM exact_avg_values", 0),
        ("SELECT avg(n) FROM exact_avg_values", 1),
    ] {
        let scanner = Arc::new(RecordingScanner::default());
        let mut pushed_engine = SqlEngine::new();
        pushed_engine.set_range_scanner(scanner.clone());
        seed_sharded_exact_avg_values(&pushed_engine).await;

        let pushed = query_cells(pushed_engine, sql).await;

        let full_engine = SqlEngine::new();
        seed_sharded_exact_avg_values(&full_engine).await;
        let full = query_cells(full_engine, sql).await;

        assert_eq!(pushed, full);
        assert!(scanner.scans().iter().any(|scan| {
            scan.partial_aggregate
                == Some(PartialAggregateSpec {
                    function: PartialAggregateFunction::AvgParts,
                    column: Some(column),
                })
        }));
    }
}

#[tokio::test]
async fn unsupported_predicate_type_falls_back_before_partial_aggregate_pushdown() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_float_metrics(&pushed_engine).await;

    let sql = "SELECT count(*) FROM float_metrics WHERE reading = 0.1::float8";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_float_metrics(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert_eq!(pushed, vec![vec![Some("1".into())]]);
    assert!(
        scanner
            .scans()
            .iter()
            .all(|scan| scan.partial_aggregate.is_none())
    );
}

#[tokio::test]
async fn float8_sum_falls_back_to_local_aggregate_path() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_float_metrics(&pushed_engine).await;

    let sql = "SELECT sum(reading) FROM float_metrics";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_float_metrics(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert!(
        scanner
            .scans()
            .iter()
            .all(|scan| scan.partial_aggregate.is_none())
    );
}

#[tokio::test]
async fn float8_avg_falls_back_to_local_aggregate_path() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_float_metrics(&pushed_engine).await;

    let pushed = query_cells(pushed_engine, "SELECT avg(reading) FROM float_metrics").await;

    let full_engine = SqlEngine::new();
    seed_sharded_float_metrics(&full_engine).await;
    let full = query_cells(full_engine, "SELECT avg(reading) FROM float_metrics").await;

    assert_eq!(pushed, full);
    assert!(
        scanner
            .scans()
            .iter()
            .all(|scan| scan.partial_aggregate.is_none())
    );
}

#[tokio::test]
async fn sql_count_star_requests_partial_count_pushdown_and_matches_full_scan() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_items(&pushed_engine).await;

    let pushed = query_cells(
        pushed_engine,
        "SELECT count(*) AS total FROM items WHERE id >= 2 AND id < 5",
    )
    .await;

    let full_engine = SqlEngine::new();
    seed_sharded_items(&full_engine).await;
    let full = query_cells(
        full_engine,
        "SELECT count(*) AS total FROM items WHERE id >= 2 AND id < 5",
    )
    .await;

    assert_eq!(pushed, full);
    assert_eq!(pushed, vec![vec![Some("3".into())]]);
    let scans = scanner.scans();
    let count_scan = scans
        .iter()
        .find(|scan| scan.partial_aggregate.is_some())
        .expect("partial count scan requested");
    assert_eq!(count_scan.table, "items");
    assert_eq!(count_scan.projection, ProjectionPushdown::All);
    assert_eq!(
        count_scan.partial_aggregate,
        Some(PartialAggregateSpec {
            function: PartialAggregateFunction::Count,
            column: None,
        })
    );
    assert!(matches!(
        count_scan.predicate,
        PredicatePushdown::Conjunctive(_)
    ));
}

#[tokio::test]
async fn sql_top_k_requests_top_k_pushdown_and_matches_full_scan() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_items(&pushed_engine).await;

    let sql = "SELECT name, id FROM items WHERE id >= 2 ORDER BY id DESC LIMIT 2";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_items(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert_eq!(
        pushed,
        vec![
            vec![Some("five".into()), Some("5".into())],
            vec![Some("four".into()), Some("4".into())],
        ]
    );
    let top_k_scan = scanner
        .scans()
        .into_iter()
        .find(|scan| scan.top_k.is_some())
        .expect("top-k scan requested");
    assert_eq!(
        top_k_scan.top_k,
        Some(TopKSpec {
            order_by: vec![TopKColumn {
                column: 0,
                asc: false,
            }],
            limit: 2,
        })
    );
    assert!(top_k_scan.partial_aggregate.is_none());
    assert!(matches!(
        top_k_scan.predicate,
        PredicatePushdown::Conjunctive(_)
    ));
}

#[tokio::test]
async fn sql_multi_key_top_k_requests_pushdown_and_matches_full_scan() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_items(&pushed_engine).await;

    let sql = "SELECT name, id FROM items ORDER BY id ASC, name DESC LIMIT 3";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_items(&full_engine).await;
    assert_eq!(pushed, query_cells(full_engine, sql).await);
    assert_eq!(
        scanner.scans().into_iter().find_map(|scan| scan.top_k),
        Some(TopKSpec {
            order_by: vec![
                TopKColumn {
                    column: 0,
                    asc: true
                },
                TopKColumn {
                    column: 1,
                    asc: false
                },
            ],
            limit: 3,
        })
    );
}

#[tokio::test]
async fn residual_where_order_limit_uses_safe_full_scan_and_keeps_correct_result() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_sharded_items(&pushed_engine).await;

    let sql = "SELECT id, name FROM items WHERE id < 3 OR name = 'five' ORDER BY id DESC LIMIT 2";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_sharded_items(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert_eq!(
        pushed,
        vec![
            vec![Some("5".into()), Some("five".into())],
            vec![Some("2".into()), Some("two".into())],
        ]
    );
    assert!(scanner.scans().iter().all(|scan| scan.top_k.is_none()));
}

#[tokio::test]
async fn nullable_order_column_order_limit_uses_safe_full_scan() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut pushed_engine = SqlEngine::new();
    pushed_engine.set_range_scanner(scanner.clone());
    seed_nullable_order_items(&pushed_engine).await;

    let sql = "SELECT id, name FROM nullable_items ORDER BY id LIMIT 2";
    let pushed = query_cells(pushed_engine, sql).await;

    let full_engine = SqlEngine::new();
    seed_nullable_order_items(&full_engine).await;
    let full = query_cells(full_engine, sql).await;

    assert_eq!(pushed, full);
    assert_eq!(
        pushed,
        vec![
            vec![Some("1".into()), Some("one".into())],
            vec![Some("2".into()), Some("two".into())],
        ]
    );
    assert!(scanner.scans().iter().all(|scan| scan.top_k.is_none()));
}

#[tokio::test]
async fn executable_pushdown_errors_do_not_silently_full_scan() {
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(Arc::new(RejectExecutableScanner));
    seed_sharded_items(&engine).await;
    let mut session = engine.connect();

    let count_error = session
        .simple_query("SELECT count(*) FROM items")
        .await
        .expect_err("partial count error is surfaced");
    assert!(
        count_error
            .to_string()
            .contains("injected partial aggregate pushdown failure")
    );

    let top_k_error = session
        .simple_query("SELECT id FROM items ORDER BY id LIMIT 1")
        .await
        .expect_err("top-k error is surfaced");
    assert!(
        top_k_error
            .to_string()
            .contains("injected top-k pushdown failure")
    );
}

#[tokio::test]
async fn unsupported_top_k_shapes_fall_back_to_safe_full_scan() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(scanner.clone());
    seed_sharded_items(&engine).await;

    for sql in [
        "SELECT DISTINCT id FROM items ORDER BY id LIMIT 2",
        "SELECT id FROM items ORDER BY id OFFSET 1 LIMIT 2",
        "SELECT id + 1 FROM items ORDER BY id + 1 LIMIT 2",
        "SELECT count(*) FROM items ORDER BY count(*) LIMIT 1",
    ] {
        query_cells(engine.clone_handle(), sql).await;
    }

    assert!(scanner.scans().iter().all(|scan| scan.top_k.is_none()));
}

#[tokio::test]
async fn unsharded_select_does_not_request_executable_pushdown() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(scanner.clone());
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE local_items (id int4, name text)")
        .await
        .expect("create local table");
    session
        .simple_query("INSERT INTO local_items VALUES (2, 'two'), (1, 'one')")
        .await
        .expect("insert local rows");

    let rows = cells(
        session
            .simple_query("SELECT id FROM local_items ORDER BY id LIMIT 1")
            .await
            .expect("query local")
            .pop()
            .expect("one result"),
    );

    assert_eq!(rows, vec![vec![Some("1".into())]]);
    assert!(
        scanner
            .scans()
            .iter()
            .all(|scan| { scan.partial_aggregate.is_none() && scan.top_k.is_none() })
    );
}
