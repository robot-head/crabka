//! Goal-based dry-run planner for Chapter Gres range balancing.

pub mod executor;
pub mod goals;
pub mod model;
pub mod planner;

pub use executor::{
    BalanceExecutor, DryRunExecutor, ExecutionError, ExecutionPolicy, ExecutionReport,
    OperationExecution, OperationStatus, RegistryLayoutExecutor, UnsupportedExecutor, execute_plan,
    registry_execution_error,
};
pub use goals::{
    BalancerConfig, CoLocationGoal, ConversionGoal, Goal, GoalContext, GoalName, GoalPriority,
    GoalToggles, LoadSkewGoal, RangeLimitGoal, SizeGoal,
};
pub use model::{
    BalanceOperation, ComputeNode, OperationKind, RangeMetrics, TablePolicy, TenantMetrics,
};
pub use planner::{
    Plan, PlanOutput, Planner, PlanningDiagnostic, StatsFreshness, StatsVersionProgress,
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_gres_control::{
        InMemoryRegistryStore, RangeBoundary, RangeLayoutEntry, SqlUser, TenantId, TenantName,
        TenantRecord, TenantRegistryStore, TenantState,
    };
    use crabka_units::{bytes, gibibytes, mebibytes, minutes};

    use super::*;

    fn range(range_id: u32, compute_id: &str, store_bytes: u64, commit_rate: u64) -> RangeMetrics {
        RangeMetrics {
            range_id,
            table_id: 10,
            start_rowid: 0,
            end_rowid: Some(1_000),
            compute_id: compute_id.to_string(),
            store_bytes: Some(store_bytes),
            checkpoint_bytes: Some(store_bytes / 2),
            commit_rate: Some(commit_rate),
            scan_bytes: Some(0),
            is_sharded: true,
            co_location_group: None,
            co_location_bucket: None,
            is_index_range: false,
        }
    }

    fn table() -> TablePolicy {
        TablePolicy {
            table_id: 10,
            table_name: "orders".to_string(),
            is_sharded: true,
            auto_shard_disabled: false,
            convert_store_bytes_threshold: bytes(10_000),
            convert_commit_rate_threshold: 10_000,
        }
    }

    fn tenant(ranges: Vec<RangeMetrics>) -> TenantMetrics {
        TenantMetrics {
            tenant_name: "blue".to_string(),
            computes: vec![
                ComputeNode {
                    compute_id: "c1".to_string(),
                },
                ComputeNode {
                    compute_id: "c2".to_string(),
                },
                ComputeNode {
                    compute_id: "c3".to_string(),
                },
            ],
            tables: vec![table()],
            ranges,
        }
    }

    fn registry_record(name: &str, endpoint: &str, wal_generation: u64) -> TenantRecord {
        TenantRecord::new(
            1,
            TenantId::try_from(name).expect("tenant id"),
            TenantName::try_from(name).expect("tenant name"),
            TenantState::Active,
            SqlUser::try_from("alice").expect("sql user"),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .expect("tenant record")
        .with_range_layout(vec![RangeLayoutEntry {
            range_id: 1,
            end_key: Some(RangeBoundary::table_start(100)),
            endpoint: endpoint.to_string(),
            wal_generation,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        }])
        .expect("range layout")
    }

    fn move_plan(from_compute_id: &str, to_compute_id: &str) -> Plan {
        Plan {
            operations: vec![BalanceOperation::Move {
                tenant_name: "tenant-a".to_string(),
                range_id: 1,
                from_compute_id: from_compute_id.to_string(),
                to_compute_id: to_compute_id.to_string(),
            }],
        }
    }

    fn context() -> GoalContext {
        GoalContext {
            size_ceiling_bytes: bytes(1_000),
            merge_floor_bytes: bytes(200),
            split_stride_rows: 100,
            load_skew_hysteresis_pct: 25,
            max_ranges_per_compute: Some(3),
            max_operations: 32,
            cooldown_epochs: 2,
            current_epoch: 10,
            cooldowns: Vec::new(),
        }
    }

    fn planner() -> Planner {
        Planner::new(vec![
            Box::new(CoLocationGoal),
            Box::new(RangeLimitGoal),
            Box::new(SizeGoal),
            Box::new(LoadSkewGoal),
            Box::new(ConversionGoal),
        ])
    }

    fn snapshot_freshness() -> StatsFreshness {
        StatsFreshness::new(minutes(1))
    }

    fn authoritative_snapshot(
        version: u64,
        sampled_at: std::time::SystemTime,
    ) -> crabka_gres_substrate::RangeStatsSnapshot {
        crabka_gres_substrate::RangeStatsSnapshot {
            version,
            sampled_at,
            ranges: vec![crabka_gres_substrate::RangeStats {
                tenant_name: "blue".to_string(),
                range_id: 1,
                row_count: None,
                store_bytes: Some(2_500),
                write_rate: Some(0),
                read_rate: Some(0),
                replication_lag_bytes: None,
            }],
        }
    }

    /// Typing the two byte thresholds must not move the wire encoding: the CLI
    /// deserialises a `BalancerConfig` from operator-supplied JSON, and the
    /// `GresBalancerThresholds` CRD mirrors these keys as bare integers.
    #[test]
    fn balancer_config_json_keeps_bare_integer_byte_thresholds() {
        const EXPECTED: &str = concat!(
            r#"{"goals":{"disabledGoals":[]},"context":{"#,
            r#""sizeCeilingBytes":1000,"mergeFloorBytes":200,"splitStrideRows":100,"#,
            r#""loadSkewHysteresisPct":25,"maxRangesPerCompute":3,"maxOperations":32,"#,
            r#""cooldownEpochs":2,"currentEpoch":10,"cooldowns":[]}}"#,
        );
        let config = BalancerConfig {
            goals: GoalToggles::default(),
            context: context(),
        };

        let json = serde_json::to_string(&config).expect("serialize config");
        let parsed: BalancerConfig = serde_json::from_str(&json).expect("deserialize config");

        check!(json == EXPECTED);
        check!(parsed.context.size_ceiling_bytes == bytes(1_000));
        check!(parsed.context.merge_floor_bytes == bytes(200));
        check!(parsed == config);
    }

    #[test]
    fn default_goal_context_keeps_its_gibibyte_and_mebibyte_thresholds() {
        let defaults = GoalContext::default();

        let json = serde_json::to_value(&defaults).expect("serialize context");

        check!(json["sizeCeilingBytes"] == serde_json::json!(1_073_741_824_u64));
        check!(json["mergeFloorBytes"] == serde_json::json!(67_108_864_u64));
        check!(defaults.size_ceiling_bytes == gibibytes(1));
        check!(defaults.merge_floor_bytes == mebibytes(64));
    }

    #[test]
    fn table_policy_json_keeps_a_bare_integer_conversion_threshold() {
        const EXPECTED: &str = concat!(
            r#"{"tableId":10,"tableName":"orders","isSharded":true,"#,
            r#""autoShardDisabled":false,"convertStoreBytesThreshold":10000,"#,
            r#""convertCommitRateThreshold":10000}"#,
        );

        let json = serde_json::to_string(&table()).expect("serialize table policy");
        let parsed: TablePolicy = serde_json::from_str(&json).expect("deserialize table policy");

        check!(json == EXPECTED);
        check!(parsed.convert_store_bytes_threshold == bytes(10_000));
        check!(parsed == table());
    }

    #[test]
    fn disabled_goal_knobs_remove_planning_surfaces() {
        let config = BalancerConfig {
            goals: GoalToggles {
                disabled_goals: vec![GoalName::RangeSize],
            },
            context: context(),
        };
        let fleet = vec![tenant(vec![range(1, "c1", 2_500, 0)])];

        let output = Planner::from_config(&config).plan(&fleet, &config.context);

        assert!(!output.plan.operations.iter().any(|operation| matches!(
            operation,
            BalanceOperation::Split { .. } | BalanceOperation::Merge { .. }
        )));
        assert!(!output.goals_applied.contains(&"range_size".to_string()));
    }

    #[test]
    fn checkpoint_reset_counters_do_not_become_zero_live_metrics() {
        let fleet = vec![tenant(vec![range(1, "c1", 2_500, 800)])];
        let checkpoint_counters = crabka_gres_substrate::CheckpointStats::default();
        assert!(checkpoint_counters.snapshot() == (0, 0));
        let snapshot = crabka_gres_substrate::RangeStatsSnapshot {
            version: 1,
            sampled_at: std::time::SystemTime::UNIX_EPOCH,
            ranges: vec![crabka_gres_substrate::RangeStats {
                tenant_name: "blue".to_string(),
                range_id: 1,
                row_count: None,
                store_bytes: None,
                write_rate: None,
                read_rate: None,
                replication_lag_bytes: None,
            }],
        };

        let output = planner().plan_with_snapshot(
            &fleet,
            &snapshot,
            &context(),
            std::time::SystemTime::UNIX_EPOCH,
            snapshot_freshness(),
            &mut StatsVersionProgress::default(),
        );

        assert!(output.plan.operations.is_empty());
        assert!(output.state_after[0].ranges[0].store_bytes.is_none());
        assert!(output.state_after[0].ranges[0].commit_rate.is_none());
    }

    #[test]
    fn authoritative_provider_metrics_enable_planning() {
        let fleet = vec![tenant(vec![range(1, "c1", 1, 1)])];
        let provider = crabka_gres_substrate::InMemoryRangeStatsProvider::new(
            crabka_gres_substrate::RangeStatsSnapshot {
                version: 3,
                sampled_at: std::time::SystemTime::UNIX_EPOCH,
                ranges: vec![crabka_gres_substrate::RangeStats {
                    tenant_name: "blue".to_string(),
                    range_id: 1,
                    row_count: None,
                    store_bytes: Some(2_500),
                    write_rate: Some(0),
                    read_rate: Some(0),
                    replication_lag_bytes: None,
                }],
            },
        );

        let output = planner().plan_with_provider(
            &fleet,
            &provider,
            &context(),
            std::time::SystemTime::UNIX_EPOCH,
            snapshot_freshness(),
            &mut StatsVersionProgress::default(),
        );

        assert!(output.plan.operations.contains(&BalanceOperation::Split {
            tenant_name: "blue".to_string(),
            table_id: 10,
            source_range_id: 1,
            split_at_rowid: 500,
        }));
    }

    #[test]
    fn stale_snapshot_abstains_without_emitting_operations() {
        let fleet = vec![tenant(vec![range(1, "c1", 1, 1)])];
        let now = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_mins(2);
        let mut progress = StatsVersionProgress::default();

        let output = planner().plan_with_snapshot(
            &fleet,
            &authoritative_snapshot(1, std::time::SystemTime::UNIX_EPOCH),
            &context(),
            now,
            snapshot_freshness(),
            &mut progress,
        );

        assert!(output.plan.operations.is_empty());
        assert!(output.diagnostic == Some(PlanningDiagnostic::StaleSnapshot));
        assert!(output.state_after == fleet);
    }

    #[test]
    fn repeated_or_regressed_snapshot_version_abstains_after_fresh_plan() {
        let fleet = vec![tenant(vec![range(1, "c1", 1, 1)])];
        let now = std::time::SystemTime::UNIX_EPOCH;
        let mut progress = StatsVersionProgress::default();

        let fresh = planner().plan_with_snapshot(
            &fleet,
            &authoritative_snapshot(5, now),
            &context(),
            now,
            snapshot_freshness(),
            &mut progress,
        );
        let repeated = planner().plan_with_snapshot(
            &fleet,
            &authoritative_snapshot(5, now),
            &context(),
            now,
            snapshot_freshness(),
            &mut progress,
        );
        let regressed = planner().plan_with_snapshot(
            &fleet,
            &authoritative_snapshot(4, now),
            &context(),
            now,
            snapshot_freshness(),
            &mut progress,
        );

        assert!(!fresh.plan.operations.is_empty());
        assert!(fresh.diagnostic.is_none());
        assert!(repeated.plan.operations.is_empty());
        assert!(repeated.diagnostic == Some(PlanningDiagnostic::NonProgressingVersion));
        assert!(regressed.plan.operations.is_empty());
        assert!(regressed.diagnostic == Some(PlanningDiagnostic::NonProgressingVersion));
    }

    #[test]
    fn dry_run_executor_preserves_planner_operations() {
        let fleet = vec![tenant(vec![range(1, "c1", 2_500, 0)])];
        let output = planner().plan(&fleet, &context());

        let report = DryRunExecutor::default().execute(&output.plan);

        assert!(report.dry_run);
        assert!(report.operations == output.plan.operations);
        assert!(report.operation_results.iter().all(|result| {
            result.status == OperationStatus::Planned && result.message.is_none()
        }));
    }

    #[test]
    fn typed_executor_applies_operations_in_plan_order() {
        let plan = Plan {
            operations: vec![
                BalanceOperation::Move {
                    tenant_name: "blue".to_string(),
                    range_id: 1,
                    from_compute_id: "c1".to_string(),
                    to_compute_id: "c2".to_string(),
                },
                BalanceOperation::Split {
                    tenant_name: "blue".to_string(),
                    table_id: 10,
                    source_range_id: 2,
                    split_at_rowid: 500,
                },
            ],
        };
        let mut executor = RecordingExecutor::default();

        let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);

        assert!(!report.dry_run);
        assert!(executor.applied == vec!["move", "split"]);
        assert!(report.is_fully_applied());
    }

    #[test]
    fn typed_executor_stops_after_first_failure_by_default() {
        let plan = Plan {
            operations: vec![
                BalanceOperation::Move {
                    tenant_name: "blue".to_string(),
                    range_id: 1,
                    from_compute_id: "c1".to_string(),
                    to_compute_id: "c2".to_string(),
                },
                BalanceOperation::Split {
                    tenant_name: "blue".to_string(),
                    table_id: 10,
                    source_range_id: 2,
                    split_at_rowid: 500,
                },
                BalanceOperation::Merge {
                    tenant_name: "blue".to_string(),
                    left_range_id: 3,
                    right_range_id: 4,
                },
            ],
        };
        let mut executor = RecordingExecutor {
            fail_operation_name: Some("split"),
            ..Default::default()
        };

        let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);

        assert!(executor.applied == vec!["move", "split"]);
        assert!(
            report
                .operation_results
                .iter()
                .map(|result| result.status)
                .collect::<Vec<_>>()
                == vec![
                    OperationStatus::Applied,
                    OperationStatus::Failed,
                    OperationStatus::Planned,
                ]
        );
        assert!(report.has_terminal_error());
    }

    #[test]
    fn unsupported_executor_reports_unsupported_operations_loudly() {
        let plan = Plan {
            operations: vec![BalanceOperation::ConvertToSharded {
                tenant_name: "blue".to_string(),
                table_id: 10,
                table_name: "orders".to_string(),
            }],
        };
        let mut executor = UnsupportedExecutor;

        let report = execute_plan(&mut executor, &plan, ExecutionPolicy::BestEffort);

        assert!(report.dry_run);
        assert!(report.operation_results[0].status == OperationStatus::Unsupported);
        assert!(
            report.operation_results[0]
                .message
                .as_deref()
                .is_some_and(|message| message.contains("convert_to_sharded"))
        );
    }

    #[cfg(any())]
    mod obsolete_metadata_only_executor_tests {
        use super::*;

        #[test]
        fn registry_move_executor_applies_live_move_and_bumps_generation() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record("tenant-a", "c1", 7))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(store);

            let report = execute_plan(
                &mut executor,
                &move_plan("c1", "c2"),
                ExecutionPolicy::StopOnFailure,
            );
            let store = executor.into_inner();
            let moved = store.get(&name).expect("record exists");

            assert!(!report.dry_run);
            assert!(report.is_fully_applied());
            assert!(moved.ranges[0].endpoint == "c2");
            assert!(moved.ranges[0].wal_generation == 8);
        }

        #[test]
        fn registry_layout_executor_reports_unsupported_conversion() {
            let store = InMemoryRegistryStore::new();
            let mut executor = RegistryLayoutExecutor::new(store);
            let plan = Plan {
                operations: vec![BalanceOperation::ConvertToSharded {
                    tenant_name: "tenant-a".to_string(),
                    table_id: 10,
                    table_name: "orders".to_string(),
                }],
            };

            let report = execute_plan(&mut executor, &plan, ExecutionPolicy::BestEffort);

            assert!(!report.dry_run);
            assert!(
                report
                    .operation_results
                    .iter()
                    .all(|result| result.status == OperationStatus::Unsupported)
            );
            assert!(report.operation_results.iter().any(|result| {
                result
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("convert_to_sharded"))
            }));
        }

        #[test]
        fn registry_layout_executor_applies_live_split_and_merge_with_version_checks() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record_with_ranges(
                    "tenant-a",
                    vec![layout_entry(
                        1,
                        Some(RangeBoundary::new(100, 1_000)),
                        "c1",
                        7,
                    )],
                    4,
                ))
                .expect("seed registry");
            let split_plan = Plan {
                operations: vec![BalanceOperation::Split {
                    tenant_name: "tenant-a".to_string(),
                    table_id: 100,
                    source_range_id: 1,
                    split_at_rowid: 500,
                }],
            };
            let mut executor = RegistryLayoutExecutor::new(store);

            let split_report =
                execute_plan(&mut executor, &split_plan, ExecutionPolicy::StopOnFailure);
            let store = executor.into_inner();
            let split_record = store.get(&name).expect("record exists");

            assert!(split_report.is_fully_applied());
            assert!(split_record.record_version == 6);
            assert!(
                split_record.ranges
                    == vec![
                        layout_entry(1, Some(RangeBoundary::new(100, 500)), "c1", 7),
                        layout_entry(2, Some(RangeBoundary::new(100, 1_000)), "c1", 7),
                    ]
            );

            let merge_plan = Plan {
                operations: vec![BalanceOperation::Merge {
                    tenant_name: "tenant-a".to_string(),
                    left_range_id: 1,
                    right_range_id: 2,
                }],
            };
            let mut executor = RegistryLayoutExecutor::new(store);

            let merge_report =
                execute_plan(&mut executor, &merge_plan, ExecutionPolicy::StopOnFailure);
            let store = executor.into_inner();
            let merged_record = store.get(&name).expect("record exists");

            assert!(merge_report.is_fully_applied());
            assert!(merged_record.record_version == 7);
            assert!(
                merged_record.ranges
                    == vec![layout_entry(
                        1,
                        Some(RangeBoundary::new(100, 1_000)),
                        "c1",
                        7,
                    )]
            );
        }

        #[test]
        fn registry_layout_executor_splits_with_source_table_when_end_boundary_is_next_table() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record_with_ranges(
                    "tenant-a",
                    vec![layout_entry(
                        1,
                        Some(RangeBoundary::table_start(200)),
                        "c1",
                        7,
                    )],
                    4,
                ))
                .expect("seed registry");
            let plan = Plan {
                operations: vec![BalanceOperation::Split {
                    tenant_name: "tenant-a".to_string(),
                    table_id: 100,
                    source_range_id: 1,
                    split_at_rowid: 500,
                }],
            };
            let mut executor = RegistryLayoutExecutor::new(store);

            let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);
            let store = executor.into_inner();
            let split_record = store.get(&name).expect("record exists");

            assert!(report.is_fully_applied());
            assert!(
                split_record.ranges
                    == vec![
                        layout_entry(1, Some(RangeBoundary::new(100, 500)), "c1", 7),
                        layout_entry(2, Some(RangeBoundary::table_start(200)), "c1", 7),
                    ]
            );
        }

        #[test]
        fn registry_layout_executor_splits_open_ended_range_with_explicit_table_id() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record_with_ranges(
                    "tenant-a",
                    vec![
                        layout_entry(1, Some(RangeBoundary::new(100, 1_000)), "c1", 7),
                        layout_entry(2, None, "c1", 9),
                    ],
                    4,
                ))
                .expect("seed registry");
            let plan = Plan {
                operations: vec![BalanceOperation::Split {
                    tenant_name: "tenant-a".to_string(),
                    table_id: 200,
                    source_range_id: 2,
                    split_at_rowid: 250,
                }],
            };
            let mut executor = RegistryLayoutExecutor::new(store);

            let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);
            let store = executor.into_inner();
            let split_record = store.get(&name).expect("record exists");

            assert!(report.is_fully_applied());
            assert!(
                split_record.ranges
                    == vec![
                        layout_entry(1, Some(RangeBoundary::new(100, 1_000)), "c1", 7),
                        layout_entry(2, Some(RangeBoundary::new(200, 250)), "c1", 9),
                        layout_entry(3, None, "c1", 9),
                    ]
            );
        }

        #[test]
        fn registry_layout_executor_rejects_stale_split_without_mutating_layout() {
            let mut inner = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            let original_ranges = vec![layout_entry(
                1,
                Some(RangeBoundary::new(100, 1_000)),
                "c1",
                7,
            )];
            inner
                .upsert(registry_record_with_ranges(
                    "tenant-a",
                    original_ranges.clone(),
                    4,
                ))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(StaleVersionStore { inner });
            let plan = Plan {
                operations: vec![
                    BalanceOperation::Split {
                        tenant_name: "tenant-a".to_string(),
                        table_id: 100,
                        source_range_id: 1,
                        split_at_rowid: 500,
                    },
                    BalanceOperation::Move {
                        tenant_name: "tenant-a".to_string(),
                        range_id: 1,
                        from_compute_id: "c1".to_string(),
                        to_compute_id: "c2".to_string(),
                    },
                ],
            };

            let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);
            let store = executor.into_inner().inner;
            let unchanged = store.get(&name).expect("record exists");

            assert!(report.has_terminal_error());
            assert!(report.operation_results[0].status == OperationStatus::Failed);
            assert!(report.operation_results[1].status == OperationStatus::Planned);
            assert!(unchanged.record_version == 5);
            assert!(unchanged.ranges == original_ranges);
        }

        #[test]
        fn registry_layout_executor_reports_unsupported_split_mutation() {
            let mut inner = InMemoryRegistryStore::new();
            inner
                .upsert(registry_record_with_ranges(
                    "tenant-a",
                    vec![layout_entry(
                        1,
                        Some(RangeBoundary::new(100, 1_000)),
                        "c1",
                        7,
                    )],
                    4,
                ))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(UnsupportedLayoutStore { inner });
            let plan = Plan {
                operations: vec![BalanceOperation::Split {
                    tenant_name: "tenant-a".to_string(),
                    table_id: 100,
                    source_range_id: 1,
                    split_at_rowid: 500,
                }],
            };

            let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);

            assert!(report.has_terminal_error());
            assert!(report.operation_results[0].status == OperationStatus::Unsupported);
            assert!(
                report.operation_results[0]
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("unsupported registry mutation"))
            );
        }

        #[test]
        fn registry_move_executor_stops_without_mutating_after_failed_move() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record("tenant-a", "actual", 7))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(store);
            let plan = Plan {
                operations: vec![
                    BalanceOperation::Move {
                        tenant_name: "tenant-a".to_string(),
                        range_id: 1,
                        from_compute_id: "stale".to_string(),
                        to_compute_id: "c2".to_string(),
                    },
                    BalanceOperation::Move {
                        tenant_name: "tenant-a".to_string(),
                        range_id: 1,
                        from_compute_id: "actual".to_string(),
                        to_compute_id: "c3".to_string(),
                    },
                ],
            };

            let report = execute_plan(&mut executor, &plan, ExecutionPolicy::StopOnFailure);
            let store = executor.into_inner();
            let unchanged = store.get(&name).expect("record exists");

            assert!(report.has_terminal_error());
            assert!(report.operation_results[0].status == OperationStatus::Failed);
            assert!(report.operation_results[1].status == OperationStatus::Planned);
            assert!(unchanged.ranges[0].endpoint == "actual");
            assert!(unchanged.ranges[0].wal_generation == 7);
        }

        #[test]
        fn registry_move_executor_treats_retried_move_as_applied_noop() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record("tenant-a", "c2", 8))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(store);

            let report = execute_plan(
                &mut executor,
                &move_plan("c1", "c2"),
                ExecutionPolicy::StopOnFailure,
            );
            let store = executor.into_inner();
            let retried = store.get(&name).expect("record exists");

            assert!(report.is_fully_applied());
            assert!(retried.ranges[0].endpoint == "c2");
            assert!(retried.ranges[0].wal_generation == 8);
        }

        #[test]
        fn registry_move_executor_reports_unsupported_registry_mutation() {
            let mut inner = InMemoryRegistryStore::new();
            inner
                .upsert(registry_record("tenant-a", "c1", 7))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(UnsupportedMoveStore { inner });

            let report = execute_plan(
                &mut executor,
                &move_plan("c1", "c2"),
                ExecutionPolicy::StopOnFailure,
            );

            assert!(report.has_terminal_error());
            assert!(report.operation_results[0].status == OperationStatus::Unsupported);
            assert!(
                report.operation_results[0]
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("unsupported registry mutation"))
            );
        }

        #[test]
        fn registry_move_executor_fails_loudly_when_generation_overflows() {
            let mut store = InMemoryRegistryStore::new();
            let name = TenantName::try_from("tenant-a").expect("tenant name");
            store
                .upsert(registry_record("tenant-a", "c1", u64::MAX))
                .expect("seed registry");
            let mut executor = RegistryLayoutExecutor::new(store);

            let report = execute_plan(
                &mut executor,
                &move_plan("c1", "c2"),
                ExecutionPolicy::StopOnFailure,
            );
            let store = executor.into_inner();
            let unchanged = store.get(&name).expect("record exists");

            assert!(report.has_terminal_error());
            assert!(report.operation_results[0].status == OperationStatus::Failed);
            assert!(
                report.operation_results[0]
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("u64::MAX"))
            );
            assert!(unchanged.ranges[0].endpoint == "c1");
            assert!(unchanged.ranges[0].wal_generation == u64::MAX);
        }

        struct UnsupportedMoveStore {
            inner: InMemoryRegistryStore,
        }

        impl TenantRegistryStore for UnsupportedMoveStore {
            fn upsert(&mut self, record: TenantRecord) -> Result<(), ControlError> {
                self.inner.upsert(record)
            }

            fn delete(&mut self, tenant: &TenantName) -> Result<(), ControlError> {
                self.inner.delete(tenant)
            }

            fn get(&self, tenant: &TenantName) -> Option<TenantRecord> {
                self.inner.get(tenant)
            }

            fn list(&self) -> Vec<TenantRecord> {
                self.inner.list()
            }

            fn mutate_range_layout_if_version(
                &mut self,
                tenant: &TenantName,
                expected_record_version: u64,
                mutation: RangeLayoutMutation,
            ) -> Result<Option<TenantRecord>, ControlError> {
                self.inner
                    .mutate_range_layout_if_version(tenant, expected_record_version, mutation)
            }
        }

        struct UnsupportedLayoutStore {
            inner: InMemoryRegistryStore,
        }

        impl TenantRegistryStore for UnsupportedLayoutStore {
            fn upsert(&mut self, record: TenantRecord) -> Result<(), ControlError> {
                self.inner.upsert(record)
            }

            fn delete(&mut self, tenant: &TenantName) -> Result<(), ControlError> {
                self.inner.delete(tenant)
            }

            fn get(&self, tenant: &TenantName) -> Option<TenantRecord> {
                self.inner.get(tenant)
            }

            fn list(&self) -> Vec<TenantRecord> {
                self.inner.list()
            }

            fn mutate_range_layout_if_version(
                &mut self,
                tenant: &TenantName,
                expected_record_version: u64,
                mutation: RangeLayoutMutation,
            ) -> Result<Option<TenantRecord>, ControlError> {
                self.inner
                    .mutate_range_layout_if_version(tenant, expected_record_version, mutation)
            }

            fn split_range_layout_if_version(
                &mut self,
                _tenant: &TenantName,
                _expected_record_version: u64,
                _split: RangeLayoutSplit,
            ) -> Result<Option<TenantRecord>, ControlError> {
                Err(ControlError::UnsupportedRegistryMutation {
                    mutation: "range compare-and-split",
                    reason: "test store rejects compare-and-split",
                })
            }

            fn merge_range_layout_if_version(
                &mut self,
                _tenant: &TenantName,
                _expected_record_version: u64,
                _merge: RangeLayoutMerge,
            ) -> Result<Option<TenantRecord>, ControlError> {
                Err(ControlError::UnsupportedRegistryMutation {
                    mutation: "range compare-and-merge",
                    reason: "test store rejects compare-and-merge",
                })
            }
        }

        struct StaleVersionStore {
            inner: InMemoryRegistryStore,
        }

        impl TenantRegistryStore for StaleVersionStore {
            fn upsert(&mut self, record: TenantRecord) -> Result<(), ControlError> {
                self.inner.upsert(record)
            }

            fn delete(&mut self, tenant: &TenantName) -> Result<(), ControlError> {
                self.inner.delete(tenant)
            }

            fn get(&self, tenant: &TenantName) -> Option<TenantRecord> {
                self.inner.get(tenant)
            }

            fn list(&self) -> Vec<TenantRecord> {
                self.inner.list()
            }

            fn mutate_range_layout_if_version(
                &mut self,
                tenant: &TenantName,
                expected_record_version: u64,
                mutation: RangeLayoutMutation,
            ) -> Result<Option<TenantRecord>, ControlError> {
                self.inner.mutate_range_layout_if_version(
                    tenant,
                    expected_record_version.saturating_add(1),
                    mutation,
                )
            }

            fn split_range_layout_if_version(
                &mut self,
                tenant: &TenantName,
                expected_record_version: u64,
                split: RangeLayoutSplit,
            ) -> Result<Option<TenantRecord>, ControlError> {
                self.inner.split_range_layout_if_version(
                    tenant,
                    expected_record_version.saturating_add(1),
                    split,
                )
            }
        }
    }

    #[test]
    fn registry_layout_executor_rejects_physical_operations_without_mutating_registry() {
        let mut store = InMemoryRegistryStore::new();
        let name = TenantName::try_from("tenant-a").expect("tenant name");
        let original = registry_record("tenant-a", "c1", 7);
        store.upsert(original.clone()).expect("seed registry");
        let mut executor = RegistryLayoutExecutor::new(store);

        let report = execute_plan(
            &mut executor,
            &move_plan("c1", "c2"),
            ExecutionPolicy::StopOnFailure,
        );
        let store = executor.into_inner();

        assert!(report.dry_run);
        assert!(report.operation_results[0].status == OperationStatus::Unsupported);
        assert!(
            report.operation_results[0]
                .message
                .as_deref()
                .is_some_and(|message| message.contains("checkpoint, copy, catch-up, and cutover"))
        );
        assert!(store.get(&name) == Some(original));
    }

    #[derive(Default)]
    struct RecordingExecutor {
        applied: Vec<&'static str>,
        fail_operation_name: Option<&'static str>,
    }

    impl BalanceExecutor for RecordingExecutor {
        fn is_dry_run(&self) -> bool {
            false
        }

        fn apply_operation(&mut self, operation: &BalanceOperation) -> Result<(), ExecutionError> {
            let operation_name = operation.operation_name();
            self.applied.push(operation_name);
            if self.fail_operation_name == Some(operation_name) {
                return Err(ExecutionError::Failed {
                    message: format!("{operation_name} hook rejected the operation"),
                });
            }
            Ok(())
        }
    }

    #[test]
    fn size_goal_recommends_split_and_merge_operations() {
        let fleet = vec![tenant(vec![
            range(1, "c1", 2_500, 0),
            range(2, "c1", 50, 0),
            range(3, "c1", 75, 0),
        ])];

        let output = planner().plan(&fleet, &context());

        check!(output.plan.operations.contains(&BalanceOperation::Split {
            tenant_name: "blue".to_string(),
            table_id: 10,
            source_range_id: 1,
            split_at_rowid: 500,
        }));
        check!(output.plan.operations.contains(&BalanceOperation::Merge {
            tenant_name: "blue".to_string(),
            left_range_id: 2,
            right_range_id: 3,
        }));
    }

    #[test]
    fn conversion_goal_honors_threshold_and_disable_knob() {
        let mut active = table();
        active.is_sharded = false;
        active.convert_store_bytes_threshold = bytes(1_000);
        let mut disabled = active.clone();
        disabled.table_id = 11;
        disabled.table_name = "audit".to_string();
        disabled.auto_shard_disabled = true;
        let mut first = range(1, "c1", 2_500, 0);
        first.is_sharded = false;
        let second = RangeMetrics {
            table_id: 11,
            ..first.clone()
        };
        let mut fixture = tenant(vec![first, second]);
        fixture.tables = vec![active, disabled];

        let output = planner().plan(&[fixture], &context());

        assert!(
            output
                .plan
                .operations
                .contains(&BalanceOperation::ConvertToSharded {
                    tenant_name: "blue".to_string(),
                    table_id: 10,
                    table_name: "orders".to_string(),
                })
        );
        assert!(!output.plan.operations.iter().any(|operation| matches!(
            operation,
            BalanceOperation::ConvertToSharded { table_id: 11, .. }
        )));
    }

    #[test]
    fn no_flapping_hysteresis_suppresses_small_load_oscillation() {
        let fleet_a = vec![tenant(vec![
            range(1, "c1", 500, 110),
            range(2, "c2", 500, 90),
        ])];
        let fleet_b = vec![tenant(vec![
            range(1, "c1", 500, 90),
            range(2, "c2", 500, 110),
        ])];

        let ctx = GoalContext {
            load_skew_hysteresis_pct: 60,
            ..context()
        };
        let output_a = planner().plan(&fleet_a, &ctx);
        let output_b = planner().plan(&fleet_b, &ctx);

        assert!(
            !output_a
                .plan
                .operations
                .iter()
                .any(BalanceOperation::is_move)
        );
        assert!(
            !output_b
                .plan
                .operations
                .iter()
                .any(BalanceOperation::is_move)
        );
    }

    #[test]
    fn converges_under_skew_by_replaying_dry_run_moves() {
        let mut fleet = vec![tenant(vec![
            range(1, "c1", 500, 800),
            range(2, "c1", 500, 600),
            range(3, "c1", 500, 400),
            range(4, "c2", 500, 20),
            range(5, "c3", 500, 20),
        ])];
        let ctx = GoalContext {
            load_skew_hysteresis_pct: 25,
            max_operations: 1,
            ..context()
        };

        let mut last = Plan::default();
        for _ in 0..6 {
            let output = planner().plan(&fleet, &ctx);
            last = output.plan;
            if last.operations.is_empty() {
                break;
            }
            for operation in &last.operations {
                operation.apply_to(&mut fleet);
            }
        }

        assert!(
            last.operations.is_empty(),
            "expected convergence, got {last:?}"
        );
    }

    #[test]
    fn co_location_and_range_limit_constraints_emit_moves() {
        let mut left = range(1, "c1", 500, 0);
        left.co_location_group = Some("tenant-customer".to_string());
        left.co_location_bucket = Some(7);
        let mut right = RangeMetrics {
            range_id: 2,
            table_id: 20,
            compute_id: "c2".to_string(),
            ..left.clone()
        };
        right.co_location_group = Some("tenant-customer".to_string());
        let fleet = vec![tenant(vec![
            left,
            right,
            range(3, "c1", 500, 0),
            range(4, "c1", 500, 0),
        ])];
        let ctx = GoalContext {
            max_ranges_per_compute: Some(2),
            ..context()
        };

        let output = planner().plan(&fleet, &ctx);

        assert!(output.plan.operations.iter().any(|operation| matches!(
            operation,
            BalanceOperation::Move { range_id: 2, from_compute_id, to_compute_id, .. }
                if from_compute_id == "c2" && to_compute_id == "c1"
        )));
        assert!(output.goals_applied.starts_with(&[
            "co_location_integrity".to_string(),
            "range_limit".to_string(),
        ]));
    }

    #[test]
    fn cooldown_blocks_repeating_the_same_operation_kind() {
        let ctx = GoalContext {
            cooldowns: vec![(1, OperationKind::Split, 12)],
            ..context()
        };
        let fleet = vec![tenant(vec![range(1, "c1", 2_500, 0)])];

        let output = planner().plan(&fleet, &ctx);

        assert!(!output.plan.operations.iter().any(|operation| matches!(
            operation,
            BalanceOperation::Split {
                source_range_id: 1,
                ..
            }
        )));
    }
}
