//! Ruler evaluation helpers.

use std::collections::BTreeMap;

use crabka_metrics::WalRecord;

use crate::PromqlError;

mod alerting;
mod config;
mod evaluation;
mod recording;
mod schedule;

pub use alerting::{
    evaluate_and_dispatch_alerting_rule, evaluate_and_dispatch_alerting_rule_group,
    evaluate_and_dispatch_alerting_rule_with_state, evaluate_and_persist_alerting_rule_group,
    evaluate_and_persist_alerting_rule_with_state,
};
#[cfg(test)]
use config::parse_duration;
pub use evaluation::{
    evaluate_and_persist_ruler_rule_group, evaluate_and_persist_ruler_rule_set,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval, evaluate_ruler_rule_group,
    evaluate_ruler_rule_set,
};
pub use recording::{
    evaluate_and_append_recording_rule, evaluate_and_append_recording_rule_group,
    evaluate_recording_rule,
};
pub use schedule::{
    RulerShard, filter_ruler_rule_set_due_for_eval, filter_ruler_rule_set_for_shard,
    filter_ruler_rule_set_for_shard_due_for_eval,
};

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

#[cfg(test)]
mod tests;
