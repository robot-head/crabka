//! KIP-932 share-group membership configuration.
use std::time::Duration;

/// Transaction isolation for share-group reads. `ReadUncommitted`, the
/// default of Kafka's `share.group.isolation.level`, exposes all records up to
/// the high watermark. `ReadCommitted` clamps reads to the last stable offset,
/// so the group never acquires uncommitted transactional records.
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
    pub max_size: usize,
    pub record_lock_duration: Duration,
    pub max_delivery_attempts: i16,
    pub max_inflight_records: i32,
    pub backlog_poll_interval: Duration,
    pub isolation_level: ShareIsolationLevel,
    pub actor_mailbox_capacity: usize,
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
            max_size: 200,
            record_lock_duration: Duration::from_secs(30),
            max_delivery_attempts: 5,
            max_inflight_records: 200,
            backlog_poll_interval: Duration::from_secs(15),
            isolation_level: ShareIsolationLevel::ReadUncommitted,
            actor_mailbox_capacity: 64,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    #[test]
    fn defaults_are_kafka_ga() {
        let c = ShareGroupConfig::default();
        check!(c.enable);
        check!(c.heartbeat_interval == Duration::from_secs(5));
        check!(c.session_timeout == Duration::from_secs(45));
        check!(c.max_size == 200);
    }

    #[test]
    fn slice_c_defaults() {
        let c = ShareGroupConfig::default();
        check!(c.record_lock_duration == std::time::Duration::from_secs(30));
        check!(c.max_delivery_attempts == 5);
        check!(c.max_inflight_records == 200);
        check!(c.backlog_poll_interval == Duration::from_secs(15));
    }

    #[test]
    fn slice_f_defaults() {
        let c = ShareGroupConfig::default();
        assert!(c.isolation_level == ShareIsolationLevel::ReadUncommitted);
        // The enum's own Default must also be ReadUncommitted.
        assert!(ShareIsolationLevel::default() == ShareIsolationLevel::ReadUncommitted);
    }
}
