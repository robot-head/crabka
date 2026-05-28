//! Static broker config for the KIP-848 next-gen consumer group protocol.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct NextGenConfig {
    /// Comma-separated list; "consumer" enables KIP-848. Default "classic,consumer".
    pub rebalance_protocols: Vec<RebalanceProtocol>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    pub assignors: Vec<String>,
    pub max_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceProtocol {
    Classic,
    Consumer,
}

impl Default for NextGenConfig {
    fn default() -> Self {
        Self {
            rebalance_protocols: vec![RebalanceProtocol::Classic, RebalanceProtocol::Consumer],
            session_timeout: Duration::from_millis(45_000),
            heartbeat_interval: Duration::from_millis(5_000),
            min_session_timeout: Duration::from_millis(45_000),
            max_session_timeout: Duration::from_millis(60_000),
            min_heartbeat_interval: Duration::from_millis(5_000),
            max_heartbeat_interval: Duration::from_millis(15_000),
            assignors: vec!["uniform".into(), "range".into()],
            max_size: 200,
        }
    }
}

impl NextGenConfig {
    pub fn next_gen_enabled(&self) -> bool {
        self.rebalance_protocols.contains(&RebalanceProtocol::Consumer)
    }

    pub fn assignor_enabled(&self, name: &str) -> bool {
        self.assignors.iter().any(|a| a == name)
    }
}
