//! KIP-932 share-coordinator (persister) configuration.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareCoordinatorConfig {
    pub state_topic_num_partitions: i32,
    pub state_topic_replication_factor: i16,
    pub state_topic_min_isr: i32,
    pub snapshot_update_records_per_snapshot: u32,
}

impl Default for ShareCoordinatorConfig {
    fn default() -> Self {
        Self {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 1,
            snapshot_update_records_per_snapshot: 50,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn defaults_match_kafka() {
        let expected = ShareCoordinatorConfig {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 1,
            snapshot_update_records_per_snapshot: 50,
        };
        assert!(ShareCoordinatorConfig::default() == expected);
    }
}
