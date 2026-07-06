use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use crabka_blockstore::Labels;
use crabka_metrics::{SamplePayload, WalRecord};

use crate::{EngineOpts, InMemoryMetricStore, PromqlEngine};

fn labels(metric: &str, job: &str) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", metric);
    labels.insert("job", job);
    labels
}

#[test]
fn parse_duration_ms_supports_all_units_and_compounds_and_rejects_bad_input() {
    // Compound multi-unit durations, single-unit coverage across the full
    // Prometheus unit set, and hard errors (`None`, never `0`) for negative,
    // empty, and unparseable input.
    for (input, want_ms) in [
        ("1h30m", Some(5_400_000)),
        ("100ms", Some(100)),
        ("5s", Some(5_000)),
        ("1w", Some(604_800_000)),
        ("1y", Some(31_536_000_000)),
        ("0", Some(0)),
        ("-5m", None),
        ("", None),
        ("5x", None),
        ("abc", None),
    ] {
        assert!(
            super::parse_duration_ms(input).ok() == want_ms,
            "case {input:?}"
        );
    }
}

#[tokio::test]
async fn alerting_rule_with_compound_for_does_not_fire_immediately() {
    // "1h30m" must parse to 90m; the alert may not fire until the series has
    // been active that long. The old single-unit parser coerced this to `0`
    // and fired on the first evaluation.
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 1h30m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 60_000 + 90 * 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::RulerAlertState::default();

    // First evaluation: the alert becomes active now (active-since = this
    // eval time). With `for: 1h30m` it must NOT fire immediately — proving
    // the compound duration parsed as 90m rather than collapsing to 0.
    let pending = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 60_000,
    )
    .await
    .expect("pending evaluation");
    assert!(pending == 0);
    assert!(sink.alerts().is_empty());

    // 90 minutes later the `for: 1h30m` window is satisfied and it fires.
    let firing = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &sink,
        &mut state,
        "tenant-a",
        &rule,
        60_000 + 90 * 60_000,
    )
    .await
    .expect("firing evaluation");
    assert!(firing == 1);
}

#[tokio::test]
async fn alerting_rule_with_negative_for_is_a_hard_error() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: -5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::RulerAlertState::default();

    let result = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 60_000,
    )
    .await;
    assert!(result.is_err());
}

#[test]
fn ruler_rule_set_filter_partitions_groups_by_tenant_namespace_and_group() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name) in [
        ("team-a", "recording"),
        ("team-a", "alerting"),
        ("team-b", "recording"),
        ("team-c", "slo"),
    ] {
        let group =
            serde_yaml::to_value(BTreeMap::from([("name", group_name)])).expect("group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }

    let shard_count = 4;
    let mut assigned = BTreeSet::new();
    for index in 1..=shard_count {
        let shard = super::RulerShard::new(index, shard_count).expect("ruler shard");
        let filtered = super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
        for (namespace, groups) in filtered {
            for (group_name, group) in groups {
                check!(assigned.insert((namespace.clone(), group_name.clone())));
                check!(
                    group
                        == rules
                            .get(&namespace)
                            .expect("namespace")
                            .get(&group_name)
                            .expect("group")
                            .clone()
                );
                check!(shard.owns_group("tenant-a", &namespace, &group_name));
                check!(!shard.owns_group("tenant-b", &namespace, &group_name));
            }
        }
    }

    assert!(
        assigned
            == BTreeSet::from([
                ("team-a".to_string(), "alerting".to_string()),
                ("team-a".to_string(), "recording".to_string()),
                ("team-b".to_string(), "recording".to_string()),
                ("team-c".to_string(), "slo".to_string()),
            ])
    );
    for (index, total) in [(0, shard_count), (shard_count + 1, shard_count), (1, 0)] {
        assert!(
            super::RulerShard::new(index, total).is_err(),
            "case ({index}, {total})"
        );
    }
}

#[derive(Default)]
struct RecordingSink {
    records: Mutex<Vec<WalRecord>>,
}

impl RecordingSink {
    fn records(&self) -> Vec<WalRecord> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl super::RecordingRuleWalSink for RecordingSink {
    async fn append_recording_rule_record(
        &self,
        record: WalRecord,
    ) -> Result<(), super::RulerWalError> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .push(record);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingAlertmanagerSink {
    alerts: Mutex<Vec<super::AlertmanagerAlert>>,
}

impl RecordingAlertmanagerSink {
    fn alerts(&self) -> Vec<super::AlertmanagerAlert> {
        self.alerts
            .lock()
            .expect("alertmanager sink poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl super::AlertmanagerSink for RecordingAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<super::AlertmanagerAlert>,
    ) -> Result<(), super::RulerWalError> {
        self.alerts
            .lock()
            .expect("alertmanager sink poisoned")
            .extend(alerts);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingRulerStateSink {
    group_records: Mutex<Vec<super::RulerGroupStateRecord>>,
    alert_records: Mutex<Vec<super::RulerAlertStateRecord>>,
}

impl RecordingRulerStateSink {
    fn group_records(&self) -> Vec<super::RulerGroupStateRecord> {
        self.group_records
            .lock()
            .expect("ruler state sink poisoned")
            .clone()
    }

    fn alert_records(&self) -> Vec<super::RulerAlertStateRecord> {
        self.alert_records
            .lock()
            .expect("ruler state sink poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl super::RulerStateSink for RecordingRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        record: super::RulerGroupStateRecord,
    ) -> Result<(), super::RulerWalError> {
        self.group_records
            .lock()
            .expect("ruler state sink poisoned")
            .push(record);
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        record: super::RulerAlertStateRecord,
    ) -> Result<(), super::RulerWalError> {
        self.alert_records
            .lock()
            .expect("ruler state sink poisoned")
            .push(record);
        Ok(())
    }
}

#[tokio::test]
async fn recording_rule_evaluation_materializes_float_samples_as_wal_records() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels("http_requests_total", "api"),
        60_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels("http_requests_total", "web"),
        60_000,
        11.0,
    );
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());

    let records = super::evaluate_recording_rule(
        &engine,
        "tenant-a",
        "job:http_requests:sum",
        "sum by (job) (http_requests_total)",
        &BTreeMap::new(),
        60_000,
    )
    .await
    .expect("recording rule evaluation");

    check!(records.len() == 2);
    check!(records.iter().all(|record| record.tenant == "tenant-a"));
    check!(records.iter().any(|record| record.labels
        == vec![
            ("__name__".to_string(), "job:http_requests:sum".to_string()),
            ("job".to_string(), "api".to_string()),
        ]
        && matches!(
            record.payload,
            SamplePayload::Float {
                timestamp_ms: 60_000,
                value: 7.0,
                start_timestamp_ms: None,
            }
        )));
    check!(records.iter().any(|record| record.labels
        == vec![
            ("__name__".to_string(), "job:http_requests:sum".to_string()),
            ("job".to_string(), "web".to_string()),
        ]
        && matches!(
            record.payload,
            SamplePayload::Float {
                timestamp_ms: 60_000,
                value: 11.0,
                start_timestamp_ms: None,
            }
        )));
}

#[tokio::test]
async fn recording_rule_append_writes_materialized_records_to_sink() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingSink::default();

    let appended = super::evaluate_and_append_recording_rule(
        &engine,
        &sink,
        "tenant-a",
        "job:up:current",
        "up",
        &BTreeMap::new(),
        60_000,
    )
    .await
    .expect("recording rule append");

    assert!(appended == 1);
    assert!(
        sink.records()
            == vec![WalRecord {
                tenant: "tenant-a".to_string(),
                labels: vec![
                    ("__name__".to_string(), "job:up:current".to_string()),
                    ("job".to_string(), "api".to_string()),
                ],
                payload: SamplePayload::Float {
                    timestamp_ms: 60_000,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                exemplars: Vec::new(),
            }]
    );
}

#[tokio::test]
async fn recording_rule_merges_rule_level_labels_into_every_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "web"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let rule_labels = BTreeMap::from([
        ("env".to_string(), "prod".to_string()),
        ("team".to_string(), "sre".to_string()),
    ]);

    let records = super::evaluate_recording_rule(
        &engine,
        "tenant-a",
        "job:up:current",
        "up",
        &rule_labels,
        60_000,
    )
    .await
    .expect("recording rule evaluation");

    assert!(records.len() == 2);
    for record in &records {
        for (name, value) in [
            ("env", "prod"),
            ("team", "sre"),
            ("__name__", "job:up:current"),
        ] {
            assert!(
                record
                    .labels
                    .contains(&(name.to_string(), value.to_string())),
                "label {name}={value}"
            );
        }
    }
}

#[tokio::test]
async fn recording_rule_fails_on_labelset_collision_after_rule_labels() {
    // Two series differ only by `job`; a rule label overwriting `job` to a
    // constant collapses them to the same labelset, which Prometheus rejects.
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "web"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let rule_labels = BTreeMap::from([("job".to_string(), "merged".to_string())]);

    let result = super::evaluate_recording_rule(
        &engine,
        "tenant-a",
        "job:up:current",
        "up",
        &rule_labels,
        60_000,
    )
    .await;

    assert!(let Err(super::PromqlError::Exec(_)) = &result);
    if let Err(super::PromqlError::Exec(message)) = result {
        assert!(message.contains("same labelset after applying rule labels"));
    }
}

#[tokio::test]
async fn recording_rule_group_append_runs_recording_rules_and_skips_alerts() {
    let group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: availability
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceDown
    expr: up == 0
",
    )
    .expect("rule group yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "web"), 60_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingSink::default();

    let appended =
        super::evaluate_and_append_recording_rule_group(&engine, &sink, "tenant-a", &group, 60_000)
            .await
            .expect("recording rule group append");

    let records = sink.records();
    check!(appended == 2);
    check!(records.len() == 2);
    check!(
        records.iter().all(
            |record| record.labels[0] == ("__name__".to_string(), "job:up:current".to_string())
        )
    );
}

#[tokio::test]
async fn alerting_rule_dispatch_sends_firing_alerts_to_alertmanager_sink() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
labels:
  severity: page
annotations:
  summary: instance is up
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "web"), 60_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();

    let dispatched =
        super::evaluate_and_dispatch_alerting_rule(&engine, &sink, "tenant-a", &rule, 60_000)
            .await
            .expect("alert dispatch");

    assert!(dispatched == 1);
    assert!(
        sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                    ("severity".to_string(), "page".to_string()),
                ]),
                annotations: BTreeMap::from([(
                    "summary".to_string(),
                    "instance is up".to_string()
                )]),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}

#[tokio::test]
async fn alerting_rule_dispatch_expands_value_and_labels_templates() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r#"
alert: InstanceUp
expr: up > 0
labels:
  detail: "v={{ $value }}"
annotations:
  summary: "{{ $labels.job }} value {{ $value }}"
  passthrough: "{{ humanize $value }}"
"#,
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();

    let dispatched =
        super::evaluate_and_dispatch_alerting_rule(&engine, &sink, "tenant-a", &rule, 60_000)
            .await
            .expect("alert dispatch");

    assert!(dispatched == 1);
    // `$value` is formatted via format_sample_value and `$labels.job` resolved
    // (in alert label values too); unknown actions like `humanize` are left
    // untouched.
    assert!(
        sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("detail".to_string(), "v=1".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::from([
                    (
                        "passthrough".to_string(),
                        "{{ humanize $value }}".to_string()
                    ),
                    ("summary".to_string(), "api value 1".to_string()),
                ]),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}

#[tokio::test]
async fn alerting_rule_state_persistence_records_active_and_cleared_alerts() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 120_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut state = super::RulerAlertState::default();

    let pending = super::evaluate_and_persist_alerting_rule_with_state(
        &engine,
        &alert_sink,
        &state_sink,
        &mut state,
        "tenant-a",
        &rule,
        60_000,
    )
    .await
    .expect("pending alert state persistence");
    let cleared = super::evaluate_and_persist_alerting_rule_with_state(
        &engine,
        &alert_sink,
        &state_sink,
        &mut state,
        "tenant-a",
        &rule,
        120_000,
    )
    .await
    .expect("cleared alert state persistence");

    let alert_labels = BTreeMap::from([
        ("__name__".to_string(), "up".to_string()),
        ("alertname".to_string(), "InstanceUp".to_string()),
        ("job".to_string(), "api".to_string()),
    ]);
    check!(pending == 0);
    check!(cleared == 0);
    check!(
        state_sink.alert_records()
            == vec![
                super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: alert_labels.clone(),
                    active_since_ms: Some(60_000),
                },
                super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: alert_labels,
                    active_since_ms: None,
                },
            ]
    );
}

#[tokio::test]
async fn ruler_alert_state_replays_compacted_records_before_evaluation() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 5m
",
    )
    .expect("alerting rule yaml");
    let alert_labels = BTreeMap::from([
        ("__name__".to_string(), "up".to_string()),
        ("alertname".to_string(), "InstanceUp".to_string()),
        ("job".to_string(), "api".to_string()),
    ]);
    let mut state = super::RulerAlertState::default();
    state.apply_record(super::RulerAlertStateRecord {
        tenant: "tenant-a".to_string(),
        rule_id: "InstanceUp\nup > 0".to_string(),
        labels: alert_labels.clone(),
        active_since_ms: Some(60_000),
    });

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();

    let firing = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 360_000,
    )
    .await
    .expect("replayed alert state evaluation");

    assert!(firing == 1);
    assert!(sink.alerts()[0].starts_at_ms == 60_000);

    state.apply_record(super::RulerAlertStateRecord {
        tenant: "tenant-a".to_string(),
        rule_id: "InstanceUp\nup > 0".to_string(),
        labels: alert_labels,
        active_since_ms: None,
    });
    let sink = RecordingAlertmanagerSink::default();
    let pending = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 360_000,
    )
    .await
    .expect("tombstoned alert state evaluation");

    assert!(pending == 0);
    assert!(sink.alerts().is_empty());
}

#[test]
fn ruler_group_state_replays_compacted_last_eval_records() {
    let mut state = super::RulerGroupState::default();
    state.apply_records(vec![
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "availability".to_string(),
            last_eval_ms: 60_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "latency".to_string(),
            last_eval_ms: 90_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "availability".to_string(),
            last_eval_ms: 120_000,
        },
    ]);

    for (tenant, namespace, group, want) in [
        ("tenant-a", "team-a", "availability", Some(120_000)),
        ("tenant-a", "team-b", "latency", Some(90_000)),
        ("tenant-b", "team-a", "availability", None),
    ] {
        assert!(
            state.last_eval_ms(tenant, namespace, group) == want,
            "case {tenant}/{namespace}/{group}"
        );
    }
}

#[test]
fn ruler_rule_set_filter_keeps_only_groups_due_for_evaluation() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name, interval) in [
        ("team-a", "new", "30s"),
        ("team-a", "not-yet", "5m"),
        ("team-b", "due", "1m"),
    ] {
        let group = serde_yaml::to_value(BTreeMap::from([
            ("name", group_name),
            ("interval", interval),
        ]))
        .expect("group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }
    let mut state = super::RulerGroupState::default();
    state.apply_records(vec![
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "not-yet".to_string(),
            last_eval_ms: 120_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "due".to_string(),
            last_eval_ms: 60_000,
        },
    ]);

    let due = super::filter_ruler_rule_set_due_for_eval("tenant-a", &rules, &state, 180_000);

    let due_group_names = due
        .iter()
        .map(|(namespace, groups)| {
            (
                namespace.clone(),
                groups.keys().cloned().collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert!(
        due_group_names
            == BTreeMap::from([
                ("team-a".to_string(), BTreeSet::from(["new".to_string()])),
                ("team-b".to_string(), BTreeSet::from(["due".to_string()])),
            ])
    );
}

#[test]
fn ruler_rule_set_filter_combines_shard_ownership_and_due_evaluation() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name, interval) in [
        ("team-a", "new", "30s"),
        ("team-a", "not-yet", "5m"),
        ("team-b", "due", "1m"),
        ("team-c", "also-due", "30s"),
    ] {
        let group = serde_yaml::to_value(BTreeMap::from([
            ("name", group_name),
            ("interval", interval),
        ]))
        .expect("group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }
    let mut state = super::RulerGroupState::default();
    state.apply_records(vec![
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "not-yet".to_string(),
            last_eval_ms: 120_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "due".to_string(),
            last_eval_ms: 60_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-c".to_string(),
            group: "also-due".to_string(),
            last_eval_ms: 90_000,
        },
    ]);
    let shard = super::RulerShard::new(1, 2).expect("ruler shard");

    let sharded = super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
    let expected = super::filter_ruler_rule_set_due_for_eval("tenant-a", &sharded, &state, 180_000);
    let scheduled = super::filter_ruler_rule_set_for_shard_due_for_eval(
        "tenant-a", &rules, &state, shard, 180_000,
    );

    assert!(scheduled == expected);
}

#[tokio::test]
async fn alerting_rule_dispatch_waits_for_for_duration_before_sending() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::RulerAlertState::default();

    let pending = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 60_000,
    )
    .await
    .expect("pending alert evaluation");
    assert!(pending == 0);
    assert!(sink.alerts().is_empty());

    let firing = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 360_000,
    )
    .await
    .expect("firing alert evaluation");
    assert!(firing == 1);
    assert!(
        sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}

#[tokio::test]
async fn firing_alert_emits_resolved_when_series_stops_matching() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 120_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let mut state = super::RulerAlertState::default();

    // First tick: fires immediately (no `for`).
    let firing_sink = RecordingAlertmanagerSink::default();
    let firing = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &firing_sink,
        &mut state,
        "tenant-a",
        &rule,
        60_000,
    )
    .await
    .expect("firing evaluation");
    assert!(firing == 1);
    assert!(firing_sink.alerts()[0].ends_at_ms == None);

    // Second tick: series drops; a resolved alert with EndsAt is emitted.
    let resolved_sink = RecordingAlertmanagerSink::default();
    let resolved = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &resolved_sink,
        &mut state,
        "tenant-a",
        &rule,
        120_000,
    )
    .await
    .expect("resolved evaluation");
    assert!(resolved == 1);
    assert!(
        resolved_sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: Some(120_000),
                generator_url: String::new(),
            }]
    );
}

#[tokio::test]
async fn keep_firing_for_holds_alert_firing_then_resolves_after_window() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
keep_firing_for: 5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 0, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 120_000, 0.0);
    store.push_float("tenant-a", labels("up", "api"), 600_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let mut state = super::RulerAlertState::default();

    // t=0: fires; keep-firing deadline armed at 0 + 5m = 300_000.
    let sink0 = RecordingAlertmanagerSink::default();
    let fired = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink0, &mut state, "tenant-a", &rule, 0,
    )
    .await
    .expect("initial firing");
    assert!(fired == 1);
    assert!(sink0.alerts()[0].ends_at_ms == None);

    // t=120s: series gone but within keep_firing_for; still firing, no EndsAt.
    let sink1 = RecordingAlertmanagerSink::default();
    let kept = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink1, &mut state, "tenant-a", &rule, 120_000,
    )
    .await
    .expect("kept firing");
    let kept_alerts = sink1.alerts();
    assert!(kept == 1);
    assert!(kept_alerts[0].ends_at_ms == None);

    // t=600s: keep-firing window (deadline 300s) elapsed; resolves with EndsAt.
    let sink2 = RecordingAlertmanagerSink::default();
    let resolved = super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink2, &mut state, "tenant-a", &rule, 600_000,
    )
    .await
    .expect("resolved after window");
    let resolved_alerts = sink2.alerts();
    assert!(resolved == 1);
    assert!(resolved_alerts[0].ends_at_ms == Some(600_000));
}

#[tokio::test]
async fn alerting_rule_group_dispatch_runs_alerts_and_skips_recording_rules() {
    let group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: mixed
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
    )
    .expect("rule group yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::RulerAlertState::default();

    let pending = super::evaluate_and_dispatch_alerting_rule_group(
        &engine, &sink, &mut state, "tenant-a", &group, 60_000,
    )
    .await
    .expect("pending group alert evaluation");
    assert!(pending == 0);
    assert!(sink.alerts().is_empty());

    let firing = super::evaluate_and_dispatch_alerting_rule_group(
        &engine, &sink, &mut state, "tenant-a", &group, 360_000,
    )
    .await
    .expect("firing group alert evaluation");
    assert!(firing == 1);
    assert!(
        sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ruler_rule_group_evaluation_appends_recordings_and_dispatches_firing_alerts() {
    let group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: mixed
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
    )
    .expect("rule group yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let mut state = super::RulerAlertState::default();

    let pending = super::evaluate_ruler_rule_group(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        "tenant-a",
        &group,
        60_000,
    )
    .await
    .expect("pending group evaluation");
    assert!(
        pending
            == super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 0,
                last_eval_ms: 60_000,
            }
    );

    let firing = super::evaluate_ruler_rule_group(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        "tenant-a",
        &group,
        360_000,
    )
    .await
    .expect("firing group evaluation");

    assert!(
        firing
            == super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 360_000,
            }
    );
    assert!(
        wal_sink.records()
            == vec![
                WalRecord {
                    tenant: "tenant-a".to_string(),
                    labels: vec![
                        ("__name__".to_string(), "job:up:current".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ],
                    payload: SamplePayload::Float {
                        timestamp_ms: 60_000,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    exemplars: Vec::new(),
                },
                WalRecord {
                    tenant: "tenant-a".to_string(),
                    labels: vec![
                        ("__name__".to_string(), "job:up:current".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ],
                    payload: SamplePayload::Float {
                        timestamp_ms: 360_000,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    exemplars: Vec::new(),
                },
            ]
    );
    assert!(
        alert_sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}

#[tokio::test]
async fn ruler_rule_set_evaluation_persists_group_last_eval_state() {
    let recording_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: recording
rules:
  - record: job:up:current
    expr: up
",
    )
    .expect("recording group yaml");
    let alerting_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
",
    )
    .expect("alerting group yaml");
    let mut rules = BTreeMap::new();
    rules
        .entry("team-a".to_string())
        .or_insert_with(BTreeMap::new)
        .insert("recording".to_string(), recording_group);
    rules
        .entry("team-b".to_string())
        .or_insert_with(BTreeMap::new)
        .insert("alerting".to_string(), alerting_group);

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 120_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut alert_state = super::RulerAlertState::default();

    let evaluation = super::evaluate_and_persist_ruler_rule_set(
        &engine,
        &wal_sink,
        &alert_sink,
        &state_sink,
        &mut alert_state,
        "tenant-a",
        &rules,
        120_000,
    )
    .await
    .expect("rule-set evaluation with state persistence");

    assert!(
        evaluation
            == super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 120_000,
            }
    );
    assert!(
        state_sink.group_records()
            == vec![
                super::RulerGroupStateRecord {
                    tenant: "tenant-a".to_string(),
                    namespace: "team-a".to_string(),
                    group: "recording".to_string(),
                    last_eval_ms: 120_000,
                },
                super::RulerGroupStateRecord {
                    tenant: "tenant-a".to_string(),
                    namespace: "team-b".to_string(),
                    group: "alerting".to_string(),
                    last_eval_ms: 120_000,
                },
            ]
    );
    assert!(
        state_sink.alert_records()
            == vec![super::RulerAlertStateRecord {
                tenant: "tenant-a".to_string(),
                rule_id: "InstanceUp\nup > 0".to_string(),
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                active_since_ms: Some(120_000),
            }]
    );
}

#[tokio::test]
async fn ruler_rule_set_scheduled_evaluation_runs_only_owned_due_groups() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name, interval, record_name) in [
        ("team-a", "new", "30s", "job:up:new"),
        ("team-a", "not-yet", "5m", "job:up:not_yet"),
        ("team-b", "due", "1m", "job:up:due"),
        ("team-c", "also-due", "30s", "job:up:also_due"),
    ] {
        let group: serde_yaml::Value = serde_yaml::from_str(&format!(
            r"
name: {group_name}
interval: {interval}
rules:
  - record: {record_name}
    expr: up
"
        ))
        .expect("recording group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }
    let mut group_state = super::RulerGroupState::default();
    group_state.apply_records(vec![
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "not-yet".to_string(),
            last_eval_ms: 120_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "due".to_string(),
            last_eval_ms: 60_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-c".to_string(),
            group: "also-due".to_string(),
            last_eval_ms: 90_000,
        },
    ]);
    let shard = super::RulerShard::new(1, 2).expect("ruler shard");
    let expected = super::filter_ruler_rule_set_for_shard_due_for_eval(
        "tenant-a",
        &rules,
        &group_state,
        shard,
        180_000,
    );
    let expected_groups = expected
        .values()
        .flat_map(|groups| groups.keys().cloned())
        .collect::<BTreeSet<_>>();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 180_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut alert_state = super::RulerAlertState::default();

    let evaluation = super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval(
        &engine,
        &wal_sink,
        &alert_sink,
        &state_sink,
        &mut alert_state,
        "tenant-a",
        &rules,
        &mut group_state,
        shard,
        180_000,
    )
    .await
    .expect("scheduled rule-set evaluation");

    assert!(evaluation.recording_records == expected_groups.len());
    assert!(
        state_sink
            .group_records()
            .iter()
            .map(|record| record.group.clone())
            .collect::<BTreeSet<_>>()
            == expected_groups
    );
    for record in state_sink.group_records() {
        assert!(
            group_state.last_eval_ms(&record.tenant, &record.namespace, &record.group)
                == Some(record.last_eval_ms)
        );
    }
    assert!(wal_sink.records().len() == expected_groups.len());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ruler_rule_set_evaluation_runs_namespaced_groups() {
    let recording_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: recording
rules:
  - record: job:up:current
    expr: up
",
    )
    .expect("recording group yaml");
    let alerting_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
    )
    .expect("alerting group yaml");
    let mut rules = BTreeMap::new();
    rules
        .entry("team-a".to_string())
        .or_insert_with(BTreeMap::new)
        .insert("recording".to_string(), recording_group);
    rules
        .entry("team-b".to_string())
        .or_insert_with(BTreeMap::new)
        .insert("alerting".to_string(), alerting_group);

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let mut state = super::RulerAlertState::default();

    let pending = super::evaluate_ruler_rule_set(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        "tenant-a",
        &rules,
        60_000,
    )
    .await
    .expect("pending rule-set evaluation");
    assert!(
        pending
            == super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 0,
                last_eval_ms: 60_000,
            }
    );

    let firing = super::evaluate_ruler_rule_set(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        "tenant-a",
        &rules,
        360_000,
    )
    .await
    .expect("firing rule-set evaluation");
    assert!(
        firing
            == super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 360_000,
            }
    );
    assert!(
        wal_sink.records()
            == vec![
                WalRecord {
                    tenant: "tenant-a".to_string(),
                    labels: vec![
                        ("__name__".to_string(), "job:up:current".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ],
                    payload: SamplePayload::Float {
                        timestamp_ms: 60_000,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    exemplars: Vec::new(),
                },
                WalRecord {
                    tenant: "tenant-a".to_string(),
                    labels: vec![
                        ("__name__".to_string(), "job:up:current".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ],
                    payload: SamplePayload::Float {
                        timestamp_ms: 360_000,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    exemplars: Vec::new(),
                },
            ]
    );
    assert!(
        alert_sink.alerts()
            == vec![super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: None,
                generator_url: String::new(),
            }]
    );
}
