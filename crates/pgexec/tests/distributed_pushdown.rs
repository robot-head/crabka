use std::sync::{Arc, Mutex};

use crabka_pgcatalog::{Column, Table};
use crabka_pgexec::{
    ColumnPredicate, JoinExecutionStrategy, JoinRangeRequest, JoinRangeResult,
    PartialAggregateFunction, PartialAggregateSpec, PredicateOp, PredicatePushdown,
    ProjectionPushdown, RangeCursor, RangeScanner, ScanPage, ScanRequest, ScannedRow, SqlEngine,
    TopKColumn, TopKSpec,
    plan_dist::{
        CheckpointMetadata, JoinInputs, JoinStrategy, PlannerConfig, SequenceCounters, Stats,
        plan_join, plan_join_for_tables, plan_scan, strict_predicate_for_filter,
    },
    scanner::{
        LocalRangeScanner, apply_executable_scan_pushdown, apply_scan_pushdown,
        finalize_partial_aggregate_rows, merge_top_k_streams,
    },
};
use crabka_units::{ByteSize, bytes, convert::ByteSizeExt as _};

#[test]
fn co_partitioning_requires_identical_hash_metadata() {
    use crabka_pgcatalog::{HashSharding, ShardingStrategy};

    let hash = |columns: &[&str], buckets, group: Option<&str>| {
        Some(ShardingStrategy::Hash(HashSharding {
            columns: columns.iter().map(|column| (*column).to_string()).collect(),
            buckets,
            co_location_group: group.map(str::to_string),
        }))
    };
    let mut left = table();
    left.sharding = hash(&["id"], 16, Some("orders"));
    let mut right = left.clone();
    right.id = 43;

    assert!(crabka_pgexec::plan_dist::tables_are_co_partitioned(
        &left, &right
    ));
    right.sharding = hash(&["id"], 32, Some("orders"));
    assert!(!crabka_pgexec::plan_dist::tables_are_co_partitioned(
        &left, &right
    ));
    right.sharding = hash(&["id"], 16, None);
    assert!(!crabka_pgexec::plan_dist::tables_are_co_partitioned(
        &left, &right
    ));
}

#[test]
fn selected_copartitioned_join_falls_back_when_catalog_proof_is_missing() {
    let stats = FakeStats {
        left: 100,
        right: 100,
        co_partitioned: true,
    };
    let left = table();
    let mut right = table();
    right.id = 43;
    right.name = "other".to_string();

    assert_eq!(
        plan_join_for_tables(
            &stats,
            PlannerConfig {
                broadcast_threshold: bytes(64),
            },
            &left,
            &right,
            &[0],
            &[0],
        ),
        JoinStrategy::Gather
    );
}

#[derive(Debug)]
struct FakeStats {
    left: u64,
    right: u64,
    co_partitioned: bool,
}

impl Stats for FakeStats {
    fn estimated_bytes(&self, table_id: u64) -> Option<u64> {
        Some(if table_id == 1 { self.left } else { self.right })
    }

    fn are_co_partitioned(&self, _left_table_id: u64, _right_table_id: u64) -> bool {
        self.co_partitioned
    }
}

#[test]
fn join_strategy_golden_prefers_broadcast_then_copartitioned_then_gather() {
    let config = PlannerConfig {
        broadcast_threshold: bytes(64),
    };
    let inputs = JoinInputs {
        left_table_id: 1,
        right_table_id: 2,
    };
    let cases = [
        (
            FakeStats {
                left: 65,
                right: 12,
                co_partitioned: true,
            },
            JoinStrategy::Broadcast { small_table_id: 2 },
        ),
        (
            FakeStats {
                left: 100,
                right: 100,
                co_partitioned: true,
            },
            JoinStrategy::CoPartitioned,
        ),
        (
            FakeStats {
                left: 100,
                right: 100,
                co_partitioned: false,
            },
            JoinStrategy::Gather,
        ),
    ];
    for (stats, expected) in cases {
        assert_eq!(plan_join(&stats, config, inputs), expected);
    }
}

#[test]
fn sequence_and_checkpoint_stats_use_live_bytes_then_checkpoint_fallback() {
    let live = SequenceCounters::new([(7, 91)]);
    assert_eq!(live.estimated_bytes(7), Some(91));
    let checkpoint = CheckpointMetadata::new([(7, 44)]);
    assert_eq!(checkpoint.estimated_bytes(7), Some(44));
}

#[tokio::test]
async fn production_engine_stats_follow_durable_table_sequence() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE runtime_stats (id int4)")
        .await
        .unwrap();
    let table = crabka_pgcatalog::get_table(engine.catalog_kv(), "runtime_stats").unwrap();
    let before = engine.join_stats().estimated_bytes(u64::from(table.id));
    session
        .simple_query("INSERT INTO runtime_stats VALUES (1), (2), (3)")
        .await
        .unwrap();
    let after = engine.join_stats().estimated_bytes(u64::from(table.id));
    assert!(
        after > before,
        "runtime sequence adapter must observe committed inserts"
    );
}
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
            group_by: Vec::new(),
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
fn k_way_top_k_merge_matches_global_order_and_bounds_output_for_random_streams() {
    let spec = TopKSpec {
        order_by: vec![TopKColumn {
            column: 0,
            asc: true,
        }],
        limit: 7,
    };
    for seed in 0_u64..64 {
        let mut streams = vec![Vec::new(), Vec::new(), Vec::new()];
        for index in 0_u64..31 {
            let value = ((seed.wrapping_mul(17) + index.wrapping_mul(23)) % 41) as i32;
            let stream = usize::try_from(index % 3).expect("stream index fits usize");
            streams[stream].push(ScannedRow {
                rowid: seed * 100 + index,
                xmin: 1,
                row: vec![Datum::Int4(value)],
            });
        }
        for stream in &mut streams {
            crabka_pgexec::scanner::apply_top_k_pushdown(stream, &spec).unwrap();
        }
        let mut expected = streams.iter().flatten().cloned().collect::<Vec<_>>();
        crabka_pgexec::scanner::apply_top_k_pushdown(&mut expected, &spec).unwrap();
        let actual = merge_top_k_streams(streams, &spec).unwrap();
        assert_eq!(actual, expected);
        assert!(actual.len() <= 7);
    }
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
                group_by: Vec::new(),
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
                group_by: Vec::new(),
            }),
            None,
        )
        .expect("empty partial aggregate applies");

        assert_eq!(pushed[0].row, vec![Datum::Null]);
    }
}

#[test]
fn grouped_partial_count_merges_range_groups_in_deterministic_key_order() {
    let spec = PartialAggregateSpec {
        function: PartialAggregateFunction::Count,
        column: Some(0),
        group_by: Vec::new(),
    }
    .grouped_by(vec![1]);
    let ranges = vec![
        vec![
            ScannedRow {
                rowid: 1,
                xmin: 1,
                row: vec![Datum::Int4(10), Datum::Text("b".into())],
            },
            ScannedRow {
                rowid: 2,
                xmin: 1,
                row: vec![Datum::Null, Datum::Text("a".into())],
            },
        ],
        vec![
            ScannedRow {
                rowid: 3,
                xmin: 1,
                row: vec![Datum::Int4(20), Datum::Text("a".into())],
            },
            ScannedRow {
                rowid: 4,
                xmin: 1,
                row: vec![Datum::Int4(30), Datum::Null],
            },
        ],
    ];

    let partials = ranges
        .into_iter()
        .flat_map(|rows| {
            apply_executable_scan_pushdown(
                rows,
                &PredicatePushdown::FullScan,
                &ProjectionPushdown::All,
                Some(&spec),
                None,
            )
            .expect("grouped owner partial")
        })
        .collect();
    let merged = finalize_partial_aggregate_rows(partials, &spec).expect("gateway merge");

    assert_eq!(
        merged.into_iter().map(|row| row.row).collect::<Vec<_>>(),
        vec![
            vec![Datum::Text("a".into()), Datum::Int8(1)],
            vec![Datum::Text("b".into()), Datum::Int8(1)],
            vec![Datum::Null, Datum::Int8(1)],
        ]
    );
}

#[test]
fn grouped_partials_match_single_range_for_random_whole_values() {
    for seed in 0_u64..64 {
        let rows = (0_u64..47)
            .map(|index| ScannedRow {
                rowid: index,
                xmin: 1,
                row: vec![
                    if (seed + index * 7).is_multiple_of(5) {
                        Datum::Null
                    } else {
                        Datum::Int4(((seed * 19 + index * 11) % 31) as i32 - 15)
                    },
                    if (seed + index).is_multiple_of(9) {
                        Datum::Null
                    } else {
                        Datum::Text(format!("g{}", (seed + index * 3) % 5))
                    },
                ],
            })
            .collect::<Vec<_>>();
        for function in [
            PartialAggregateFunction::Count,
            PartialAggregateFunction::Sum,
            PartialAggregateFunction::Min,
            PartialAggregateFunction::Max,
            PartialAggregateFunction::AvgParts,
        ] {
            let spec = PartialAggregateSpec {
                function,
                column: Some(0),
                group_by: vec![1],
            };
            let expected = finalize_partial_aggregate_rows(
                apply_executable_scan_pushdown(
                    rows.clone(),
                    &PredicatePushdown::FullScan,
                    &ProjectionPushdown::All,
                    Some(&spec),
                    None,
                )
                .unwrap(),
                &spec,
            )
            .unwrap();
            let partials = (0..4)
                .flat_map(|range| {
                    apply_executable_scan_pushdown(
                        rows.iter().skip(range).step_by(4).cloned().collect(),
                        &PredicatePushdown::FullScan,
                        &ProjectionPushdown::All,
                        Some(&spec),
                        None,
                    )
                    .unwrap()
                })
                .collect();
            let actual = finalize_partial_aggregate_rows(partials, &spec).unwrap();
            assert_eq!(actual, expected, "seed={seed}, function={function:?}");
        }
    }
}

#[test]
fn partial_avg_merges_sum_count_across_uneven_ranges_and_preserves_null_semantics() {
    let spec = PartialAggregateSpec {
        function: PartialAggregateFunction::AvgParts,
        column: Some(0),
        group_by: Vec::new(),
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
    joins: Mutex<Vec<JoinRangeRequest>>,
}

impl RecordingScanner {
    fn scans(&self) -> Vec<RecordedScan> {
        self.scans.lock().expect("scan log").clone()
    }

    fn joins(&self) -> Vec<JoinRangeRequest> {
        self.joins.lock().expect("join log").clone()
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

    fn join(&self, request: JoinRangeRequest) -> Result<JoinRangeResult, crabka_pgexec::ExecError> {
        self.joins.lock().expect("join log").push(request);
        Err(crabka_pgexec::ExecError::Unsupported(
            "recording scanner requests deterministic local fallback".into(),
        ))
    }
}

#[tokio::test]
async fn sql_inner_equi_join_dispatches_selected_broadcast_request() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(scanner.clone());
    engine.set_join_stats(Arc::new(SequenceCounters::new([(1, 1)])));
    engine.set_join_strategy_config(PlannerConfig {
        broadcast_threshold: ByteSize::from_bytes(u64::MAX),
    });
    let mut session = engine.connect();
    session
        .simple_query(
            "CREATE TABLE left_t (id int4, value text) SHARDED; \
             CREATE TABLE right_t (id int4, value text) SHARDED; \
             INSERT INTO left_t VALUES (1, 'l'); \
             INSERT INTO right_t VALUES (1, 'r'); \
             SELECT left_t.value, right_t.value FROM left_t JOIN right_t ON left_t.id = right_t.id",
        )
        .await
        .expect("join falls back locally after dispatch");

    let joins = scanner.joins();
    assert_eq!(joins.len(), 1);
    assert!(matches!(
        joins[0].strategy,
        JoinExecutionStrategy::BroadcastLeft | JoinExecutionStrategy::BroadcastRight
    ));
    assert_ne!(joins[0].read_ts, 0);
}

#[tokio::test]
async fn sql_copartitioned_join_requires_the_join_key_to_match_hash_metadata() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(scanner.clone());
    engine.set_join_stats(Arc::new(FakeStats {
        left: 100,
        right: 100,
        co_partitioned: true,
    }));
    engine.set_join_strategy_config(PlannerConfig {
        broadcast_threshold: ByteSize::ZERO,
    });
    engine
        .connect()
        .simple_query(
            "CREATE TABLE left_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 4 COLOCATED WITH pair; \
             CREATE TABLE right_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 4 COLOCATED WITH pair; \
             SELECT * FROM left_t JOIN right_t ON left_t.value = right_t.value",
        )
        .await
        .expect("unsupported scanner falls back locally");

    assert_eq!(scanner.joins()[0].strategy, JoinExecutionStrategy::Gather);
}

#[tokio::test]
async fn sql_copartitioned_join_uses_catalog_proof_when_stats_only_estimate_sizes() {
    let scanner = Arc::new(RecordingScanner::default());
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(scanner.clone());
    engine.set_join_stats(Arc::new(SequenceCounters::new([(1, 100), (2, 100)])));
    engine.set_join_strategy_config(PlannerConfig {
        broadcast_threshold: ByteSize::ZERO,
    });
    engine
        .connect()
        .simple_query(
            "CREATE TABLE left_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 4 COLOCATED WITH pair; \
             CREATE TABLE right_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 4 COLOCATED WITH pair; \
             SELECT * FROM left_t JOIN right_t ON left_t.id = right_t.id",
        )
        .await
        .expect("unsupported scanner falls back locally");

    assert_eq!(
        scanner.joins()[0].strategy,
        JoinExecutionStrategy::CoPartitioned
    );
}

#[derive(Debug)]
struct MaterializedJoinScanner {
    left: Vec<crabka_pgexec::JoinRow>,
    right: Vec<crabka_pgexec::JoinRow>,
    joins: Mutex<Vec<JoinRangeRequest>>,
}

impl RangeScanner for MaterializedJoinScanner {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, crabka_pgexec::ExecError> {
        LocalRangeScanner.scan(request)
    }

    fn join(&self, request: JoinRangeRequest) -> Result<JoinRangeResult, crabka_pgexec::ExecError> {
        self.joins.lock().expect("join log").push(request.clone());
        crabka_pgexec::scanner::execute_materialized_join(&request, &self.left, &self.right)
    }
}

fn encoded_join_rows(rows: &[Vec<Datum>]) -> Vec<crabka_pgexec::JoinRow> {
    rows.iter()
        .map(|row| crabka_pgexec::JoinRow {
            tuple: crabka_pgmvcc::version::encode_tuple(0, 0, row),
        })
        .collect()
}

#[tokio::test]
async fn sql_join_strategies_dispatch_and_match_local_whole_rows() {
    let generated = |side: &str, salt: u64| {
        (0..64)
            .map(|seed| {
                let random = (seed * 1_103_515_245 + salt) % 17;
                let key = if random.is_multiple_of(7) {
                    Datum::Null
                } else {
                    Datum::Int4(i32::try_from(random % 9).expect("small key"))
                };
                vec![key, Datum::Text(format!("{side}-{seed:02}"))]
            })
            .collect::<Vec<_>>()
    };
    let left = generated("l", 12_345);
    let right = generated("r", 54_321);
    let values_sql = |rows: &[Vec<Datum>]| {
        rows.iter()
            .map(|row| match row.as_slice() {
                [Datum::Int4(key), Datum::Text(value)] => format!("({key}, '{value}')"),
                [Datum::Null, Datum::Text(value)] => format!("(NULL, '{value}')"),
                _ => unreachable!("generated row shape"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let cases = [
        (
            FakeStats {
                left: 1,
                right: 100,
                co_partitioned: false,
            },
            ByteSize::from_bytes(u64::MAX),
            JoinExecutionStrategy::BroadcastLeft,
        ),
        (
            FakeStats {
                left: 100,
                right: 100,
                co_partitioned: true,
            },
            ByteSize::ZERO,
            JoinExecutionStrategy::CoPartitioned,
        ),
        (
            FakeStats {
                left: 100,
                right: 100,
                co_partitioned: false,
            },
            ByteSize::ZERO,
            JoinExecutionStrategy::CoPartitioned,
        ),
    ];
    for (stats, threshold, expected_strategy) in cases {
        let scanner = Arc::new(MaterializedJoinScanner {
            left: encoded_join_rows(&left),
            right: encoded_join_rows(&right),
            joins: Mutex::new(Vec::new()),
        });
        let mut pushed = SqlEngine::new();
        pushed.set_range_scanner(scanner.clone());
        pushed.set_join_stats(Arc::new(stats));
        pushed.set_join_strategy_config(PlannerConfig {
            broadcast_threshold: threshold,
        });
        let local = SqlEngine::new();
        let ddl = format!(
            "CREATE TABLE left_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 4 COLOCATED WITH pair; \
             CREATE TABLE right_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 4 COLOCATED WITH pair; \
             INSERT INTO left_t VALUES {}; INSERT INTO right_t VALUES {}",
            values_sql(&left),
            values_sql(&right)
        );
        for engine in [&pushed, &local] {
            engine
                .connect()
                .simple_query(&ddl)
                .await
                .expect("seed tables");
        }
        let sql = "SELECT l.id, l.value, r.value FROM left_t l JOIN right_t r ON l.id = r.id ORDER BY l.id, l.value, r.value";
        let pushed_rows = query_cells(pushed.clone_handle(), sql).await;
        let local_rows = query_cells(local, sql).await;
        assert_eq!(pushed_rows, local_rows);
        let joins = scanner.joins.lock().expect("join log");
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].strategy, expected_strategy);
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

#[derive(Debug, Default)]
struct PagingScanner {
    scan_calls: std::sync::atomic::AtomicUsize,
    page_calls: Arc<std::sync::atomic::AtomicUsize>,
}

struct PagingCursor {
    next: u64,
    page_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl RangeCursor for PagingCursor {
    async fn next_page(&mut self, max_rows: usize) -> Result<ScanPage, crabka_pgexec::ExecError> {
        self.page_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let rows = (0..max_rows)
            .map(|_| {
                let value = self.next;
                self.next += 1;
                ScannedRow {
                    rowid: value,
                    xmin: 1,
                    row: vec![Datum::Int4(i32::try_from(value).expect("test value fits"))],
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(ScanPage {
            rows,
            is_last: false,
        })
    }
}

impl RangeScanner for PagingScanner {
    fn scan(&self, _request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, crabka_pgexec::ExecError> {
        self.scan_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Vec::new())
    }

    fn scan_cursor<'a>(
        &'a self,
        _request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, crabka_pgexec::ExecError> {
        Ok(Box::new(PagingCursor {
            next: 1,
            page_calls: Arc::clone(&self.page_calls),
        }))
    }
}

#[tokio::test]
async fn simple_select_limit_stops_native_cursor_before_materialization() {
    use crabka_pgwire::engine::{CollectingResultSink, Session};

    let scanner = Arc::new(PagingScanner::default());
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(scanner.clone());
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE items (id int4) SHARDED")
        .await
        .expect("table setup");
    let mut session = engine.connect();
    let mut sink = CollectingResultSink::default();

    session
        .simple_query_into("SELECT id FROM items LIMIT 1", 2, &mut sink)
        .await
        .expect("streamed select");
    let results = sink.finish().expect("valid result pages");

    assert_eq!(
        cells(results.into_iter().next().expect("one result")),
        vec![vec![Some("1".into())]]
    );
    assert_eq!(
        scanner.page_calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        scanner.scan_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
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
                    group_by: Vec::new(),
                })
        }));
    }
}

#[tokio::test]
async fn sql_grouped_aggregates_request_partial_pushdown_and_match_full_scan() {
    for function in ["count", "sum", "min", "max", "avg"] {
        let scanner = Arc::new(RecordingScanner::default());
        let mut pushed_engine = SqlEngine::new();
        pushed_engine.set_range_scanner(scanner.clone());
        seed_sharded_metrics(&pushed_engine).await;
        let sql =
            format!("SELECT label, {function}(amount) FROM metrics GROUP BY label ORDER BY label");
        let pushed = query_cells(pushed_engine, &sql).await;

        let full_engine = SqlEngine::new();
        seed_sharded_metrics(&full_engine).await;
        let full = query_cells(full_engine, &sql).await;

        assert_eq!(pushed, full, "{function}");
        assert!(scanner.scans().iter().any(|scan| {
            scan.partial_aggregate
                .as_ref()
                .is_some_and(|spec| spec.group_by == vec![2])
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
                group_by: Vec::new(),
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
                group_by: Vec::new(),
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
                    group_by: Vec::new(),
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
            group_by: Vec::new(),
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
