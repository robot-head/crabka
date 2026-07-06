use std::collections::BTreeMap;

use crabka_blockstore::Labels;

use super::{
    AlertStateKey, AlertmanagerAlert, AlertmanagerSink, NoopRulerStateSink, RulerAlertState,
    RulerAlertStateRecord, RulerStateSink,
    config::{yaml_duration_ms, yaml_optional_string, yaml_required_string, yaml_string_map},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

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
