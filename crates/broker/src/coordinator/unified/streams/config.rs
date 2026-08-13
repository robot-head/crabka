//! KIP-1071 Streams rebalance-protocol configuration.
use std::{collections::BTreeMap, time::Duration};

pub const KEY_SESSION_TIMEOUT_MS: &str = "streams.session.timeout.ms";
pub const KEY_HEARTBEAT_INTERVAL_MS: &str = "streams.heartbeat.interval.ms";
pub const KEY_ACCEPTABLE_RECOVERY_LAG: &str = "streams.acceptable.recovery.lag";
pub const KEY_NUM_WARMUP_REPLICAS: &str = "streams.num.warmup.replicas";
pub const KEY_NUM_STANDBY_REPLICAS: &str = "streams.num.standby.replicas";
pub const KEY_TASK_OFFSET_INTERVAL_MS: &str = "streams.task.offset.interval.ms";
pub const KEY_ASSIGNOR_NAME: &str = "streams.assignor.name";
pub const KEY_SHARE_AUTO_OFFSET_RESET: &str = "share.auto.offset.reset";

pub const GROUP_CONFIG_KEYS: [&str; 8] = [
    KEY_SESSION_TIMEOUT_MS,
    KEY_HEARTBEAT_INTERVAL_MS,
    KEY_ACCEPTABLE_RECOVERY_LAG,
    KEY_NUM_WARMUP_REPLICAS,
    KEY_NUM_STANDBY_REPLICAS,
    KEY_TASK_OFFSET_INTERVAL_MS,
    KEY_ASSIGNOR_NAME,
    KEY_SHARE_AUTO_OFFSET_RESET,
];

/// Server-side task-assignor selection for a streams group. `Auto` (the Kafka
/// default) picks `HighlyAvailable` when the topology has any stateful
/// subtopology (a state-changelog topic) and `Sticky` otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsAssignorKind {
    #[default]
    Auto,
    /// Minimise task movement; active-only, no standby/warmup.
    Sticky,
    /// Place standby replicas + warm up state migrations for fault tolerance.
    HighlyAvailable,
}

impl StreamsAssignorKind {
    #[must_use]
    pub fn config_name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Sticky => "sticky",
            Self::HighlyAvailable => "highly_available",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "auto" => Ok(Self::Auto),
            "sticky" => Ok(Self::Sticky),
            "highly_available" | "highly-available" => Ok(Self::HighlyAvailable),
            _ => Err(format!(
                "{KEY_ASSIGNOR_NAME} must be `auto`, `sticky`, or `highly_available`"
            )),
        }
    }
}

/// KIP-1071 streams-group membership and assignment configuration. Static
/// broker values provide defaults; GROUP resources can override the supported
/// `streams.*` keys for one group through `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupConfig {
    /// Config-level kill switch. The real gate is the `streams.version`
    /// feature (KIP-1071 early access, default-disabled). This switch lets an
    /// operator turn the protocol off even where the feature is finalized.
    pub enable: bool,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    /// Default replication factor when an internal-topic spec leaves it unset.
    pub internal_topic_replication_factor: i16,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    /// Max members per group.
    pub max_size: usize,
    /// `num.standby.replicas`: standby copies per stateful task.
    pub num_standby_replicas: i32,
    /// `max.warmup.replicas`: cap on concurrent warmup tasks. A warmup task
    /// migrates state.
    pub num_warmup_replicas: i32,
    /// `acceptable.recovery.lag`: the maximum changelog lag in records at which
    /// a warmup task is caught up. The assignor can then promote the task to
    /// active or standby.
    pub acceptable_recovery_lag: i64,
    /// How often a member reports task offsets, so the assignor can evaluate
    /// warmup catch-up. This is `task_offset_interval_ms` in the heartbeat
    /// response.
    pub task_offset_interval: Duration,
    /// Server-side assignor selection.
    pub assignor: StreamsAssignorKind,
    pub actor_mailbox_capacity: usize,
}

impl Default for StreamsGroupConfig {
    fn default() -> Self {
        Self {
            enable: true,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            internal_topic_replication_factor: 3,
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_size: 200,
            // Kafka GA defaults: no standby copies, up to 2 warmups,
            // acceptable lag 10k records.
            num_standby_replicas: 0,
            num_warmup_replicas: 2,
            acceptable_recovery_lag: 10_000,
            task_offset_interval: Duration::from_secs(30),
            assignor: StreamsAssignorKind::Auto,
            actor_mailbox_capacity: 64,
        }
    }
}

impl StreamsGroupConfig {
    /// Apply a persisted GROUP resource override map to these broker defaults.
    ///
    /// # Errors
    /// Returns a message suitable for `INVALID_CONFIG` when a key is unknown,
    /// a value cannot be parsed, or a timeout falls outside broker bounds.
    pub fn with_group_overrides(
        &self,
        overrides: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let mut out = self.clone();
        for (key, value) in overrides {
            match key.as_str() {
                KEY_SESSION_TIMEOUT_MS => {
                    out.session_timeout = parse_positive_millis(key, value)?;
                }
                KEY_HEARTBEAT_INTERVAL_MS => {
                    out.heartbeat_interval = parse_positive_millis(key, value)?;
                }
                KEY_ACCEPTABLE_RECOVERY_LAG => {
                    out.acceptable_recovery_lag = parse_nonnegative(key, value)?;
                }
                KEY_NUM_WARMUP_REPLICAS => {
                    out.num_warmup_replicas = parse_nonnegative(key, value)?;
                }
                KEY_NUM_STANDBY_REPLICAS => {
                    out.num_standby_replicas = parse_nonnegative(key, value)?;
                }
                KEY_TASK_OFFSET_INTERVAL_MS => {
                    out.task_offset_interval = parse_positive_millis(key, value)?;
                }
                KEY_ASSIGNOR_NAME => out.assignor = StreamsAssignorKind::parse(value)?,
                KEY_SHARE_AUTO_OFFSET_RESET if value == "earliest" => {}
                KEY_SHARE_AUTO_OFFSET_RESET => {
                    return Err(format!(
                        "{KEY_SHARE_AUTO_OFFSET_RESET} currently supports only `earliest`"
                    ));
                }
                _ => return Err(format!("unknown group config `{key}`")),
            }
        }
        if !(out.min_session_timeout..=out.max_session_timeout).contains(&out.session_timeout) {
            return Err(format!(
                "{KEY_SESSION_TIMEOUT_MS} must be between {} and {} ms",
                out.min_session_timeout.as_millis(),
                out.max_session_timeout.as_millis()
            ));
        }
        if !(out.min_heartbeat_interval..=out.max_heartbeat_interval)
            .contains(&out.heartbeat_interval)
        {
            return Err(format!(
                "{KEY_HEARTBEAT_INTERVAL_MS} must be between {} and {} ms",
                out.min_heartbeat_interval.as_millis(),
                out.max_heartbeat_interval.as_millis()
            ));
        }
        Ok(out)
    }

    /// Effective values exposed by `DescribeConfigs` for a GROUP resource.
    #[must_use]
    pub fn group_config_values(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                KEY_SESSION_TIMEOUT_MS.into(),
                self.session_timeout.as_millis().to_string(),
            ),
            (
                KEY_HEARTBEAT_INTERVAL_MS.into(),
                self.heartbeat_interval.as_millis().to_string(),
            ),
            (
                KEY_ACCEPTABLE_RECOVERY_LAG.into(),
                self.acceptable_recovery_lag.to_string(),
            ),
            (
                KEY_NUM_WARMUP_REPLICAS.into(),
                self.num_warmup_replicas.to_string(),
            ),
            (
                KEY_NUM_STANDBY_REPLICAS.into(),
                self.num_standby_replicas.to_string(),
            ),
            (
                KEY_TASK_OFFSET_INTERVAL_MS.into(),
                self.task_offset_interval.as_millis().to_string(),
            ),
            (KEY_ASSIGNOR_NAME.into(), self.assignor.config_name().into()),
            (KEY_SHARE_AUTO_OFFSET_RESET.into(), "earliest".into()),
        ])
    }
}

fn parse_positive_millis(key: &str, value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<u64>()
        .map_err(|_| format!("{key} must be a positive integer"))?;
    if millis == 0 {
        return Err(format!("{key} must be positive"));
    }
    Ok(Duration::from_millis(millis))
}

fn parse_nonnegative<T>(key: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr + PartialOrd + Default,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| format!("{key} must be a nonnegative integer"))?;
    if parsed < T::default() {
        return Err(format!("{key} must be nonnegative"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_are_kafka_ga() {
        assert!(
            StreamsGroupConfig::default()
                == StreamsGroupConfig {
                    enable: true,
                    session_timeout: Duration::from_secs(45),
                    heartbeat_interval: Duration::from_secs(5),
                    internal_topic_replication_factor: 3,
                    min_session_timeout: Duration::from_secs(45),
                    max_session_timeout: Duration::from_mins(1),
                    min_heartbeat_interval: Duration::from_secs(5),
                    max_heartbeat_interval: Duration::from_secs(15),
                    max_size: 200,
                    num_standby_replicas: 0,
                    num_warmup_replicas: 2,
                    acceptable_recovery_lag: 10_000,
                    task_offset_interval: Duration::from_secs(30),
                    assignor: StreamsAssignorKind::Auto,
                    actor_mailbox_capacity: 64,
                }
        );
    }

    #[test]
    fn group_overrides_are_validated_and_applied() {
        let overrides = BTreeMap::from([
            (KEY_SESSION_TIMEOUT_MS.into(), "50000".into()),
            (KEY_HEARTBEAT_INTERVAL_MS.into(), "6000".into()),
            (KEY_NUM_STANDBY_REPLICAS.into(), "1".into()),
            (KEY_ASSIGNOR_NAME.into(), "highly_available".into()),
        ]);
        let got = StreamsGroupConfig::default()
            .with_group_overrides(&overrides)
            .expect("valid overrides");
        assert!(got.session_timeout == Duration::from_secs(50));
        assert!(got.heartbeat_interval == Duration::from_secs(6));
        assert!(got.num_standby_replicas == 1);
        assert!(got.assignor == StreamsAssignorKind::HighlyAvailable);
    }

    #[test]
    fn group_overrides_reject_unknown_and_out_of_bounds_values() {
        let unknown = BTreeMap::from([("streams.unknown".into(), "1".into())]);
        assert!(
            StreamsGroupConfig::default()
                .with_group_overrides(&unknown)
                .is_err()
        );
        let too_short = BTreeMap::from([(KEY_SESSION_TIMEOUT_MS.into(), "1000".into())]);
        assert!(
            StreamsGroupConfig::default()
                .with_group_overrides(&too_short)
                .is_err()
        );
    }
}
