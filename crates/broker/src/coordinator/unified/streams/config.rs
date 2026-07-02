//! KIP-1071 Streams rebalance-protocol configuration.
use std::time::Duration;

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

/// KIP-1071 streams-group membership + assignment configuration. Sourced from
/// static broker defaults; per-group `IncrementalAlterConfigs` overrides
/// (`group.streams.*`) are not yet implemented (deferred — no GROUP-resource
/// config store exists).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupConfig {
    /// Config-level kill switch. The real gate is the `streams.version`
    /// feature (KIP-1071 early access, default-disabled); this lets an operator
    /// turn the protocol off even where the feature is finalized.
    pub enable: bool,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    /// Max number of streams groups (0 = unlimited).
    pub max_groups: usize,
    /// Max members per group.
    pub max_size: usize,
    /// `num.standby.replicas`: standby copies per stateful task.
    pub num_standby_replicas: i32,
    /// `max.warmup.replicas`: cap on concurrent warmup (state-migration) tasks.
    pub num_warmup_replicas: i32,
    /// `acceptable.recovery.lag`: max changelog lag (records) at which a warmup
    /// task is considered caught up and promotable to active/standby.
    pub acceptable_recovery_lag: i64,
    /// How often a member reports task offsets so the assignor can evaluate
    /// warmup catch-up (`task_offset_interval_ms` in the heartbeat response).
    pub task_offset_interval: Duration,
    /// Server-side assignor selection.
    pub assignor: StreamsAssignorKind,
}

impl Default for StreamsGroupConfig {
    fn default() -> Self {
        Self {
            enable: true,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_groups: 0,
            max_size: 200,
            // Kafka GA defaults: no standby copies, up to 2 warmups,
            // acceptable lag 10k records.
            num_standby_replicas: 0,
            num_warmup_replicas: 2,
            acceptable_recovery_lag: 10_000,
            task_offset_interval: Duration::from_secs(30),
            assignor: StreamsAssignorKind::Auto,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn defaults_are_kafka_ga() {
        assert!(
            StreamsGroupConfig::default()
                == StreamsGroupConfig {
                    enable: true,
                    session_timeout: Duration::from_secs(45),
                    heartbeat_interval: Duration::from_secs(5),
                    min_session_timeout: Duration::from_secs(45),
                    max_session_timeout: Duration::from_mins(1),
                    min_heartbeat_interval: Duration::from_secs(5),
                    max_heartbeat_interval: Duration::from_secs(15),
                    max_groups: 0,
                    max_size: 200,
                    num_standby_replicas: 0,
                    num_warmup_replicas: 2,
                    acceptable_recovery_lag: 10_000,
                    task_offset_interval: Duration::from_secs(30),
                    assignor: StreamsAssignorKind::Auto,
                }
        );
    }
}
