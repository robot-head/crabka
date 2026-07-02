//! Ruler evaluation helpers.

use std::collections::BTreeMap;

use crabka_blockstore::Labels;
use crabka_metrics::{SamplePayload, WalRecord};

use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

/// Errors from writing ruler output to the metrics WAL.
#[derive(Debug, thiserror::Error)]
pub enum RulerWalError {
    #[error("ruler wal append failed: {0}")]
    Append(String),
}

impl From<RulerWalError> for PromqlError {
    fn from(error: RulerWalError) -> Self {
        Self::Exec(error.to_string())
    }
}

/// Sink for recording rule output records.
#[async_trait::async_trait]
pub trait RecordingRuleWalSink: Send + Sync {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError>;
}

/// One alert payload ready for an Alertmanager-compatible API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlertmanagerAlert {
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub starts_at_ms: i64,
    pub ends_at_ms: Option<i64>,
    pub generator_url: String,
}

/// Sink for firing alert notifications.
#[async_trait::async_trait]
pub trait AlertmanagerSink: Send + Sync {
    async fn dispatch_alerts(&self, alerts: Vec<AlertmanagerAlert>) -> Result<(), RulerWalError>;
}

/// Rebuildable state for one ruler group evaluation.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RulerGroupStateRecord {
    pub tenant: String,
    pub namespace: String,
    pub group: String,
    pub last_eval_ms: i64,
}

/// Last-evaluation state for ruler groups, rebuildable from compacted records.
#[derive(Debug, Default)]
pub struct RulerGroupState {
    last_eval_ms: BTreeMap<RulerGroupStateKey, i64>,
}

impl RulerGroupState {
    /// Apply one compacted group-state record to the in-memory group tracker.
    pub fn apply_record(&mut self, record: RulerGroupStateRecord) {
        self.last_eval_ms.insert(
            RulerGroupStateKey {
                tenant: record.tenant,
                namespace: record.namespace,
                group: record.group,
            },
            record.last_eval_ms,
        );
    }

    /// Rebuild group state from compacted group-state records.
    pub fn apply_records<I>(&mut self, records: I)
    where
        I: IntoIterator<Item = RulerGroupStateRecord>,
    {
        for record in records {
            self.apply_record(record);
        }
    }

    #[must_use]
    pub fn last_eval_ms(&self, tenant: &str, namespace: &str, group: &str) -> Option<i64> {
        self.last_eval_ms
            .get(&RulerGroupStateKey {
                tenant: tenant.to_string(),
                namespace: namespace.to_string(),
                group: group.to_string(),
            })
            .copied()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RulerGroupStateKey {
    tenant: String,
    namespace: String,
    group: String,
}

/// Rebuildable state for one alert instance.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RulerAlertStateRecord {
    pub tenant: String,
    pub rule_id: String,
    pub labels: BTreeMap<String, String>,
    pub active_since_ms: Option<i64>,
}

/// Sink for compacted ruler state records.
#[async_trait::async_trait]
pub trait RulerStateSink: Send + Sync {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError>;

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError>;
}

struct NoopRulerStateSink;

#[async_trait::async_trait]
impl RulerStateSink for NoopRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        _record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        _record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        Ok(())
    }
}

/// Summary of one ruler rule-group evaluation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RulerGroupEvaluation {
    pub recording_records: usize,
    pub alerts_dispatched: usize,
    pub last_eval_ms: i64,
}

/// One ruler shard for deterministic rule-group ownership.
///
/// Shards are one-based to match Mimir's shard notation: `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RulerShard {
    pub index: usize,
    pub total: usize,
}

impl RulerShard {
    pub fn new(index: usize, total: usize) -> Result<Self, PromqlError> {
        if total == 0 {
            return Err(PromqlError::Plan(
                "ruler shard total must be positive".into(),
            ));
        }
        if index == 0 || index > total {
            return Err(PromqlError::Plan(format!(
                "ruler shard index must be between 1 and {total}"
            )));
        }
        Ok(Self { index, total })
    }

    #[must_use]
    pub fn owns_group(self, tenant: &str, namespace: &str, group_name: &str) -> bool {
        let buckets = self.total as u64;
        let shard_index =
            usize::try_from(stable_hash_parts(&[tenant, namespace, group_name]) % buckets)
                .unwrap_or(0);
        shard_index == self.index - 1
    }
}

/// Pending/firing alert state for ruler evaluations.
#[derive(Debug, Default)]
pub struct RulerAlertState {
    active_since_ms: BTreeMap<AlertStateKey, i64>,
    /// For each alert instance that has reached the firing state, the wall-clock
    /// deadline (`eval_time + keep_firing_for`) until which it must keep firing
    /// after its series stops matching. Presence also marks "this instance has
    /// fired", so a series that only ever pended does not emit a resolved alert.
    ///
    /// NOTE: this is in-memory session state only — it is intentionally not part
    /// of the compacted [`RulerAlertStateRecord`], so a ruler restart mid-window
    /// loses the keep-firing deadline. Full durable keep-firing tracking is
    /// deferred (it would require a wire change to the persisted record).
    keep_firing_until_ms: BTreeMap<AlertStateKey, i64>,
}

impl RulerAlertState {
    /// Apply one compacted alert-state record to the in-memory alert tracker.
    pub fn apply_record(&mut self, record: RulerAlertStateRecord) {
        let key = AlertStateKey {
            tenant: record.tenant,
            rule_id: record.rule_id,
            labels: record.labels,
        };
        match record.active_since_ms {
            Some(active_since_ms) => {
                self.active_since_ms.insert(key, active_since_ms);
            }
            None => {
                self.active_since_ms.remove(&key);
            }
        }
    }

    /// Rebuild alert state from compacted alert-state records.
    pub fn apply_records<I>(&mut self, records: I)
    where
        I: IntoIterator<Item = RulerAlertStateRecord>,
    {
        for record in records {
            self.apply_record(record);
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AlertStateKey {
    tenant: String,
    rule_id: String,
    labels: BTreeMap<String, String>,
}

/// Evaluate one recording rule and materialize the result as metrics WAL records.
///
/// Each output series gets `__name__` rewritten to `record_name` and the
/// rule-level `labels` merged on top (rule labels win), matching Prometheus.
/// If two output samples collapse to the same labelset after that rewrite the
/// rule fails — Prometheus rejects this as "vector contains metrics with the
/// same labelset after applying rule labels" — instead of writing duplicate
/// WAL records.
pub async fn evaluate_recording_rule<S: MetricStore>(
    engine: &PromqlEngine<S>,
    tenant: &str,
    record_name: &str,
    expr: &str,
    rule_labels: &BTreeMap<String, String>,
    eval_time_ms: i64,
) -> Result<Vec<WalRecord>, PromqlError> {
    let result = engine.query_instant(tenant, expr, eval_time_ms).await?;
    let QueryResult::InstantVector(samples) = result else {
        return Err(PromqlError::Exec(
            "recording rule expression must evaluate to an instant vector".into(),
        ));
    };

    let mut seen_fingerprints = std::collections::BTreeSet::new();
    let mut records = Vec::with_capacity(samples.len());
    for sample in samples {
        let labels = recording_labels(sample.labels, record_name, rule_labels);
        if !seen_fingerprints.insert(labels.fingerprint()) {
            return Err(PromqlError::Exec(
                "vector contains metrics with the same labelset after applying rule labels".into(),
            ));
        }
        let payload = match sample.value {
            SampleValue::Float(value) => SamplePayload::Float {
                timestamp_ms: sample.ts_ms,
                value,
                start_timestamp_ms: None,
            },
            SampleValue::Histogram(hist) => SamplePayload::Hist {
                timestamp_ms: sample.ts_ms,
                hist,
            },
        };
        records.push(WalRecord {
            tenant: tenant.to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            payload,
            exemplars: Vec::new(),
        });
    }
    Ok(records)
}

/// Evaluate one recording rule and append its materialized samples to the WAL sink.
pub async fn evaluate_and_append_recording_rule<S, W>(
    engine: &PromqlEngine<S>,
    sink: &W,
    tenant: &str,
    record_name: &str,
    expr: &str,
    rule_labels: &BTreeMap<String, String>,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
{
    let records =
        evaluate_recording_rule(engine, tenant, record_name, expr, rule_labels, eval_time_ms)
            .await?;
    let count = records.len();
    for record in records {
        sink.append_recording_rule_record(record).await?;
    }
    Ok(count)
}

/// Evaluate all recording rules in one rule group and append their outputs.
pub async fn evaluate_and_append_recording_rule_group<S, W>(
    engine: &PromqlEngine<S>,
    sink: &W,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
{
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "recording rule group must contain rules".into(),
        ));
    };

    let mut appended = 0;
    for rule in rules {
        let Some(record_name) = yaml_optional_string(rule, "record") else {
            continue;
        };
        let expr = yaml_required_string(rule, "expr")?;
        let rule_labels = yaml_string_map(rule, "labels");
        appended += evaluate_and_append_recording_rule(
            engine,
            sink,
            tenant,
            &record_name,
            &expr,
            &rule_labels,
            eval_time_ms,
        )
        .await?;
    }
    Ok(appended)
}

/// Evaluate one alerting rule and dispatch active alerts to Alertmanager.
pub async fn evaluate_and_dispatch_alerting_rule<S, A>(
    engine: &PromqlEngine<S>,
    sink: &A,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    let mut state = RulerAlertState::default();
    evaluate_and_dispatch_alerting_rule_with_state(
        engine,
        sink,
        &mut state,
        tenant,
        rule,
        eval_time_ms,
    )
    .await
}

/// Evaluate one alerting rule, track pending state, and dispatch only firing alerts.
pub async fn evaluate_and_dispatch_alerting_rule_with_state<S, A>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state: &mut RulerAlertState,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    evaluate_alerting_rule_with_state_and_sink(
        engine,
        sink,
        &NoopRulerStateSink,
        state,
        tenant,
        rule,
        eval_time_ms,
    )
    .await
}

/// Evaluate one alerting rule, persist alert state, and dispatch only firing alerts.
pub async fn evaluate_and_persist_alerting_rule_with_state<S, A, R>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state_sink: &R,
    state: &mut RulerAlertState,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    evaluate_alerting_rule_with_state_and_sink(
        engine,
        sink,
        state_sink,
        state,
        tenant,
        rule,
        eval_time_ms,
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn evaluate_alerting_rule_with_state_and_sink<S, A, R>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state_sink: &R,
    state: &mut RulerAlertState,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let Some(alert_name) = yaml_optional_string(rule, "alert") else {
        return Ok(0);
    };
    let expr = yaml_required_string(rule, "expr")?;
    let result = engine.query_instant(tenant, &expr, eval_time_ms).await?;
    let QueryResult::InstantVector(samples) = result else {
        return Ok(0);
    };

    let rule_labels = yaml_string_map(rule, "labels");
    let annotations = yaml_string_map(rule, "annotations");
    let duration_ms = yaml_duration_ms(rule, "for")?;
    let keep_firing_for_ms = yaml_duration_ms(rule, "keep_firing_for")?;
    let rule_id = format!("{alert_name}\n{expr}");
    let mut active_keys = Vec::new();
    let mut active_records = Vec::new();
    let mut alerts = Vec::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let mut labels = labels_to_map(&sample.labels);
        labels.insert("alertname".to_string(), alert_name.clone());
        labels.extend(rule_labels.clone());
        // Expand `$value`/`$labels` in alert label values against the sample's
        // series labels, matching Prometheus alert templating.
        let labels = expand_alert_label_map(&labels, value, &sample.labels);
        let key = AlertStateKey {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
        };
        active_keys.push(key.clone());
        let starts_at_ms = *state
            .active_since_ms
            .entry(key.clone())
            .or_insert(eval_time_ms);
        active_records.push(RulerAlertStateRecord {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
            active_since_ms: Some(starts_at_ms),
        });
        if eval_time_ms.saturating_sub(starts_at_ms) < duration_ms {
            // Still pending: not firing yet, so it cannot be kept firing.
            state.keep_firing_until_ms.remove(&key);
            continue;
        }
        // Firing: (re)arm the keep-firing deadline so that if the series stops
        // matching on a later tick the alert keeps firing for `keep_firing_for`.
        state
            .keep_firing_until_ms
            .insert(key, eval_time_ms.saturating_add(keep_firing_for_ms));
        let annotations = expand_alert_label_map(&annotations, value, &sample.labels);
        alerts.push(AlertmanagerAlert {
            labels,
            annotations,
            starts_at_ms,
            ends_at_ms: None,
            generator_url: String::new(),
        });
    }

    // Reconcile alert instances whose series stopped matching this tick.
    //
    // Prometheus only notifies for instances that previously *fired*: a pending
    // instance that disappears is dropped silently. A previously-firing instance
    // is either kept firing (within its `keep_firing_for` window) or resolved
    // with `EndsAt = eval_time`.
    let cleared_keys = state
        .active_since_ms
        .keys()
        .filter(|key| key.tenant == tenant && key.rule_id == rule_id && !active_keys.contains(key))
        .cloned()
        .collect::<Vec<_>>();
    let mut cleared_records = Vec::new();
    let mut kept_firing_keys = Vec::new();
    for key in cleared_keys {
        match state.keep_firing_until_ms.get(&key).copied() {
            // Within the keep-firing window: the alert stays firing. Retain its
            // active state and re-emit it as still-firing (no `EndsAt`).
            Some(until_ms) if until_ms > eval_time_ms => {
                let starts_at_ms = state
                    .active_since_ms
                    .get(&key)
                    .copied()
                    .unwrap_or(eval_time_ms);
                kept_firing_keys.push(key.clone());
                active_records.push(RulerAlertStateRecord {
                    tenant: key.tenant.clone(),
                    rule_id: key.rule_id.clone(),
                    labels: key.labels.clone(),
                    active_since_ms: Some(starts_at_ms),
                });
                alerts.push(AlertmanagerAlert {
                    labels: key.labels.clone(),
                    annotations: BTreeMap::new(),
                    starts_at_ms,
                    ends_at_ms: None,
                    generator_url: String::new(),
                });
            }
            // Had fired and the keep-firing window has elapsed (or was zero):
            // emit a resolved alert and tombstone the instance.
            Some(_) => {
                let starts_at_ms = state
                    .active_since_ms
                    .get(&key)
                    .copied()
                    .unwrap_or(eval_time_ms);
                state.keep_firing_until_ms.remove(&key);
                cleared_records.push(RulerAlertStateRecord {
                    tenant: key.tenant.clone(),
                    rule_id: key.rule_id.clone(),
                    labels: key.labels.clone(),
                    active_since_ms: None,
                });
                alerts.push(AlertmanagerAlert {
                    labels: key.labels.clone(),
                    annotations: BTreeMap::new(),
                    starts_at_ms,
                    ends_at_ms: Some(eval_time_ms),
                    generator_url: String::new(),
                });
            }
            // Only ever pending: drop silently, no notification.
            None => {
                cleared_records.push(RulerAlertStateRecord {
                    tenant: key.tenant.clone(),
                    rule_id: key.rule_id.clone(),
                    labels: key.labels.clone(),
                    active_since_ms: None,
                });
            }
        }
    }
    for record in active_records.into_iter().chain(cleared_records) {
        state_sink.persist_ruler_alert_state(record).await?;
    }
    // Retain the active instances plus any instance still inside its keep-firing
    // window; tombstone everything else for this rule.
    state.active_since_ms.retain(|key, _| {
        key.tenant != tenant
            || key.rule_id != rule_id
            || active_keys.contains(key)
            || kept_firing_keys.contains(key)
    });
    let count = alerts.len();
    if count > 0 {
        sink.dispatch_alerts(alerts).await?;
    }
    Ok(count)
}

/// Evaluate all alerting rules in one rule group and dispatch firing alerts.
pub async fn evaluate_and_dispatch_alerting_rule_group<S, A>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state: &mut RulerAlertState,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "alerting rule group must contain rules".into(),
        ));
    };

    let mut dispatched = 0;
    for rule in rules {
        if yaml_optional_string(rule, "alert").is_none() {
            continue;
        }
        dispatched += evaluate_and_dispatch_alerting_rule_with_state(
            engine,
            sink,
            state,
            tenant,
            rule,
            eval_time_ms,
        )
        .await?;
    }
    Ok(dispatched)
}

/// Evaluate all alerting rules in one rule group, persist alert state, and dispatch firing alerts.
pub async fn evaluate_and_persist_alerting_rule_group<S, A, R>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state_sink: &R,
    state: &mut RulerAlertState,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "alerting rule group must contain rules".into(),
        ));
    };

    let mut dispatched = 0;
    for rule in rules {
        if yaml_optional_string(rule, "alert").is_none() {
            continue;
        }
        dispatched += evaluate_and_persist_alerting_rule_with_state(
            engine,
            sink,
            state_sink,
            state,
            tenant,
            rule,
            eval_time_ms,
        )
        .await?;
    }
    Ok(dispatched)
}

/// Evaluate one mixed ruler rule group: recording outputs then alert dispatch.
pub async fn evaluate_ruler_rule_group<S, W, A>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
{
    let recording_records =
        evaluate_and_append_recording_rule_group(engine, wal_sink, tenant, group, eval_time_ms)
            .await?;
    let alerts_dispatched = evaluate_and_dispatch_alerting_rule_group(
        engine,
        alert_sink,
        alert_state,
        tenant,
        group,
        eval_time_ms,
    )
    .await?;
    Ok(RulerGroupEvaluation {
        recording_records,
        alerts_dispatched,
        last_eval_ms: eval_time_ms,
    })
}

/// Evaluate one mixed ruler rule group and persist alert state records.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate_and_persist_ruler_rule_group<S, W, A, R>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    state_sink: &R,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let recording_records =
        evaluate_and_append_recording_rule_group(engine, wal_sink, tenant, group, eval_time_ms)
            .await?;
    let alerts_dispatched = evaluate_and_persist_alerting_rule_group(
        engine,
        alert_sink,
        state_sink,
        alert_state,
        tenant,
        group,
        eval_time_ms,
    )
    .await?;
    Ok(RulerGroupEvaluation {
        recording_records,
        alerts_dispatched,
        last_eval_ms: eval_time_ms,
    })
}

/// Evaluate all ruler rule groups for one tenant.
pub async fn evaluate_ruler_rule_set<S, W, A>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
{
    let mut total = RulerGroupEvaluation::default();
    for namespace_groups in rules.values() {
        for group in namespace_groups.values() {
            let evaluation = evaluate_ruler_rule_group(
                engine,
                wal_sink,
                alert_sink,
                alert_state,
                tenant,
                group,
                eval_time_ms,
            )
            .await?;
            total.recording_records += evaluation.recording_records;
            total.alerts_dispatched += evaluation.alerts_dispatched;
            total.last_eval_ms = evaluation.last_eval_ms;
        }
    }
    Ok(total)
}

/// Evaluate all ruler rule groups for one tenant and persist compactable group state.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate_and_persist_ruler_rule_set<S, W, A, R>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    state_sink: &R,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let mut total = RulerGroupEvaluation::default();
    for (namespace, namespace_groups) in rules {
        for (group_name, group) in namespace_groups {
            let evaluation = evaluate_and_persist_ruler_rule_group(
                engine,
                wal_sink,
                alert_sink,
                state_sink,
                alert_state,
                tenant,
                group,
                eval_time_ms,
            )
            .await?;
            state_sink
                .persist_ruler_group_state(RulerGroupStateRecord {
                    tenant: tenant.to_string(),
                    namespace: namespace.clone(),
                    group: group_name.clone(),
                    last_eval_ms: evaluation.last_eval_ms,
                })
                .await?;
            total.recording_records += evaluation.recording_records;
            total.alerts_dispatched += evaluation.alerts_dispatched;
            total.last_eval_ms = evaluation.last_eval_ms;
        }
    }
    Ok(total)
}

/// Evaluate this shard's due ruler rule groups for one tenant and persist state.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval<S, W, A, R>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    state_sink: &R,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &mut RulerGroupState,
    shard: RulerShard,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let scheduled = filter_ruler_rule_set_for_shard_due_for_eval(
        tenant,
        rules,
        group_state,
        shard,
        eval_time_ms,
    );
    let evaluation = evaluate_and_persist_ruler_rule_set(
        engine,
        wal_sink,
        alert_sink,
        state_sink,
        alert_state,
        tenant,
        &scheduled,
        eval_time_ms,
    )
    .await?;
    for (namespace, namespace_groups) in &scheduled {
        for group_name in namespace_groups.keys() {
            group_state.apply_record(RulerGroupStateRecord {
                tenant: tenant.to_string(),
                namespace: namespace.clone(),
                group: group_name.clone(),
                last_eval_ms: eval_time_ms,
            });
        }
    }
    Ok(evaluation)
}

/// Return the rule groups owned by one ruler shard for a tenant.
#[must_use]
pub fn filter_ruler_rule_set_for_shard(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    shard: RulerShard,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for (namespace, namespace_groups) in rules {
        let groups = namespace_groups
            .iter()
            .filter(|(group_name, _)| shard.owns_group(tenant, namespace, group_name))
            .map(|(group_name, group)| (group_name.clone(), group.clone()))
            .collect::<BTreeMap<_, _>>();
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}

/// Return rule groups whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for (namespace, namespace_groups) in rules {
        let groups = namespace_groups
            .iter()
            .filter(|(group_name, group)| {
                ruler_group_due_for_eval(
                    tenant,
                    namespace,
                    group_name,
                    group,
                    group_state,
                    eval_time_ms,
                )
            })
            .map(|(group_name, group)| (group_name.clone(), group.clone()))
            .collect::<BTreeMap<_, _>>();
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}

/// Return rule groups owned by one shard whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_for_shard_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    shard: RulerShard,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let sharded = filter_ruler_rule_set_for_shard(tenant, rules, shard);
    filter_ruler_rule_set_due_for_eval(tenant, &sharded, group_state, eval_time_ms)
}

fn ruler_group_due_for_eval(
    tenant: &str,
    namespace: &str,
    group_name: &str,
    group: &serde_yaml::Value,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> bool {
    let Some(last_eval_ms) = group_state.last_eval_ms(tenant, namespace, group_name) else {
        return true;
    };
    // A malformed `interval` is a config error; skip the group rather than
    // treating an unparseable value as `0` and re-evaluating every tick. The
    // `for`/`expr` paths surface the same parse error as a hard failure.
    let Ok(interval_ms) = yaml_duration_ms(group, "interval") else {
        return false;
    };
    eval_time_ms.saturating_sub(last_eval_ms) >= interval_ms
}

fn recording_labels(
    mut labels: Labels,
    record_name: &str,
    rule_labels: &BTreeMap<String, String>,
) -> Labels {
    labels.insert("__name__", record_name);
    // Rule-level labels are applied on top of the series labels (rule labels
    // win), matching Prometheus recording-rule label semantics.
    for (name, value) in rule_labels {
        labels.insert(name.clone(), value.clone());
    }
    labels
}

fn labels_to_map(labels: &Labels) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Expand `{{ $value }}` / `{{ $labels.NAME }}` in every value of an alert
/// label or annotation map, resolving `$labels` against the firing sample's
/// series labels.
fn expand_alert_label_map(
    map: &BTreeMap<String, String>,
    value: f64,
    series_labels: &Labels,
) -> BTreeMap<String, String> {
    map.iter()
        .map(|(name, text)| {
            let expanded = crate::http_api::expand_alert_template(text, value, series_labels);
            (name.clone(), expanded)
        })
        .collect()
}

fn yaml_string_map(value: &serde_yaml::Value, key: &str) -> BTreeMap<String, String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map(|mapping| {
            mapping
                .iter()
                .filter_map(|(key, value)| Some((key.as_str()?, value.as_str()?)))
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a Prometheus duration for the given rule field, surfacing malformed
/// values as a hard error to the caller. A missing field is `0` (no duration);
/// an empty, negative, or otherwise unparseable value is rejected rather than
/// silently coerced to `0` (which would make `for`/`interval` fire immediately).
fn yaml_duration_ms(value: &serde_yaml::Value, key: &str) -> Result<i64, PromqlError> {
    match yaml_optional_string(value, key) {
        Some(duration) => parse_duration_ms(&duration),
        None => Ok(0),
    }
}

/// Parse a Prometheus duration string into milliseconds.
///
/// Supports the full Prometheus unit set (`ms`, `s`, `m`, `h`, `d`, `w`, `y`)
/// and compound durations such as `1h30m`. Mirrors the conformance harness'
/// `parse_duration_ms`. Empty, negative, or unparseable input is a hard error.
fn parse_duration_ms(duration: &str) -> Result<i64, PromqlError> {
    let src = duration.trim();
    if src.is_empty() {
        return Err(PromqlError::Exec("empty duration".into()));
    }
    if src == "0" {
        return Ok(0);
    }
    if src.starts_with('-') {
        return Err(PromqlError::Exec(format!("negative duration `{src}`")));
    }

    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = src.as_bytes();

    while index < bytes.len() {
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if start == index {
            return Err(PromqlError::Exec(format!("invalid duration `{src}`")));
        }
        let amount = src[start..index]
            .parse::<i64>()
            .map_err(|err| PromqlError::Exec(format!("invalid duration amount `{src}`: {err}")))?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &src[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return Err(PromqlError::Exec(format!("invalid duration unit `{unit}`"))),
        };
        total_ms += amount
            .checked_mul(multiplier)
            .ok_or_else(|| PromqlError::Exec(format!("duration overflow `{src}`")))?;
    }

    Ok(total_ms)
}

fn yaml_optional_string(value: &serde_yaml::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
}

fn yaml_required_string(value: &serde_yaml::Value, key: &str) -> Result<String, PromqlError> {
    yaml_optional_string(value, key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PromqlError::Exec(format!("recording rule must contain a non-empty {key}")))
}

fn stable_hash_parts(parts: &[&str]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for part in parts {
        for byte in part.as_bytes().iter().copied().chain(std::iter::once(0)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    use assert2::assert;
    use assert2::check;
    use crabka_metrics::{SamplePayload, WalRecord};

    use crabka_blockstore::Labels;

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

        let appended = super::evaluate_and_append_recording_rule_group(
            &engine, &sink, "tenant-a", &group, 60_000,
        )
        .await
        .expect("recording rule group append");

        let records = sink.records();
        check!(appended == 2);
        check!(records.len() == 2);
        check!(records.iter().all(
            |record| record.labels[0] == ("__name__".to_string(), "job:up:current".to_string())
        ));
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
        let expected =
            super::filter_ruler_rule_set_due_for_eval("tenant-a", &sharded, &state, 180_000);
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
}
