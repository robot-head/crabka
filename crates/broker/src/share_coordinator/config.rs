//! KIP-932 share-coordinator (persister) configuration.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareCoordinatorConfig {
    pub state_topic_num_partitions: i32,
    pub state_topic_replication_factor: i16,
    pub state_topic_min_isr: i32,
    pub snapshot_update_records_per_snapshot: u32,
    pub recovery_read_max_bytes: usize,
}

impl Default for ShareCoordinatorConfig {
    fn default() -> Self {
        Self {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 1,
            snapshot_update_records_per_snapshot: 50,
            recovery_read_max_bytes: 1_048_576,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_match_kafka() {
        let expected = ShareCoordinatorConfig {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 1,
            snapshot_update_records_per_snapshot: 50,
            recovery_read_max_bytes: 1_048_576,
        };
        assert!(ShareCoordinatorConfig::default() == expected);
    }
}
