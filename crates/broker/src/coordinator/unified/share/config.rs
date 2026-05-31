//! KIP-932 share-group membership configuration.
use std::time::Duration;

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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    #[test]
    fn defaults_are_kafka_ga() {
        let c = ShareGroupConfig::default();
        assert!(c.enable);
        assert!(c.heartbeat_interval == Duration::from_secs(5));
        assert!(c.session_timeout == Duration::from_secs(45));
        assert!(c.max_size == 200);
    }
}
