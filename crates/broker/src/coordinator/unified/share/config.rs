//! KIP-932 share-group membership configuration.
use std::time::Duration;

/// Transaction isolation applied to share-group reads. `ReadUncommitted`
/// (Kafka's `share.group.isolation.level` default) exposes all records up to
/// the high watermark; `ReadCommitted` clamps reads to the last stable offset
/// so uncommitted transactional records are never acquired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShareIsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

#[derive(Debug, Clone)]
pub struct ShareGroupConfig {
    pub enable: bool,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    pub max_groups: usize,
    pub max_size: usize,
    pub record_lock_duration: Duration,
    pub max_delivery_attempts: i16,
    pub max_inflight_records: i32,
    pub isolation_level: ShareIsolationLevel,
}

impl Default for ShareGroupConfig {
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
            record_lock_duration: Duration::from_secs(30),
            max_delivery_attempts: 5,
            max_inflight_records: 200,
            isolation_level: ShareIsolationLevel::ReadUncommitted,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    #[test]
    fn defaults_are_kafka_ga() {
        let c = ShareGroupConfig::default();
        assert!(
            (
                (
                    c.enable,
                    c.session_timeout,
                    c.heartbeat_interval,
                    c.min_session_timeout,
                    c.max_session_timeout,
                    c.min_heartbeat_interval,
                    c.max_heartbeat_interval,
                ),
                (
                    c.max_groups,
                    c.max_size,
                    c.record_lock_duration,
                    c.max_delivery_attempts,
                    c.max_inflight_records,
                    c.isolation_level,
                ),
            ) == (
                (
                    true,
                    Duration::from_secs(45),
                    Duration::from_secs(5),
                    Duration::from_secs(45),
                    Duration::from_mins(1),
                    Duration::from_secs(5),
                    Duration::from_secs(15),
                ),
                (
                    0,
                    200,
                    Duration::from_secs(30),
                    5,
                    200,
                    ShareIsolationLevel::ReadUncommitted,
                ),
            )
        );
        assert!(ShareIsolationLevel::default() == ShareIsolationLevel::ReadUncommitted);
    }
}
