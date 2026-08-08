use std::collections::BTreeMap;

use super::{
    AlertmanagerSink, RecordingRuleWalSink, RulerAlertState, RulerGroupEvaluation, RulerGroupState,
    RulerGroupStateRecord, RulerShard, RulerStateSink, evaluate_and_append_recording_rule_group,
    evaluate_and_dispatch_alerting_rule_group, evaluate_and_persist_alerting_rule_group,
    filter_ruler_rule_set_for_shard_due_for_eval,
};
use crate::{MetricStore, PromqlEngine, PromqlError};

/// Evaluates one mixed ruler rule group: recording outputs, then alert dispatch.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
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

/// Evaluates one mixed ruler rule group and persists alert state records.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_group<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
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
    let (wal_sink, alert_sink, state_sink) = sinks;
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

/// Evaluates all ruler rule groups for one tenant.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
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

/// Evaluates all ruler rule groups for one tenant and persists compactable group state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
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
    let (wal_sink, alert_sink, state_sink) = sinks;
    let mut total = RulerGroupEvaluation::default();
    for (namespace, namespace_groups) in rules {
        for (group_name, group) in namespace_groups {
            let evaluation = evaluate_and_persist_ruler_rule_group(
                engine,
                (wal_sink, alert_sink, state_sink),
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

/// Evaluates this shard's due ruler rule groups for one tenant and persists state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    schedule: (&mut RulerGroupState, RulerShard, i64),
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    let (group_state, shard, eval_time_ms) = schedule;
    let scheduled = filter_ruler_rule_set_for_shard_due_for_eval(
        tenant,
        rules,
        group_state,
        shard,
        eval_time_ms,
    );
    let evaluation = evaluate_and_persist_ruler_rule_set(
        engine,
        (wal_sink, alert_sink, state_sink),
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
