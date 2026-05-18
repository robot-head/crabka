//! Hard goal: enforce a per-broker `cpu_cores` limit from the
//! capacity config.
//!
//! **Stub in slice 43d.** `propose` returns empty and `is_satisfied`
//! returns true unconditionally — per-partition CPU-usage data is
//! not available until slice 43e/43f wires `metrics_scraper`. The
//! struct, registry entry, and config-field reads ship now so the
//! later slice can replace the body mechanically.

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement};

pub struct CpuCapacity;

impl CpuCapacity {
    pub const NAME: &'static str = "CpuCapacity";
}

impl Goal for CpuCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, _state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        // Stub: 43e/43f wires per-partition CPU usage and the real logic.
        Vec::new()
    }

    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        // Stub: 43e/43f replaces this with a real capacity-vs-usage check.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::{BrokerView, PartitionView};
    use std::sync::Arc;

    fn ctx() -> GoalContext {
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                cpu_cores: Some(8.0),
                ..Default::default()
            },
        );
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities { by_broker: by }),
        }
    }

    #[test]
    fn stub_returns_empty_regardless_of_state() {
        let state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![BrokerView {
                id: 1,
                host: "h1".into(),
                port: 9092,
                rack: None,
            }],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            }],
            in_flight_reassignments: vec![],
        };
        assert!(CpuCapacity.propose(&state, &ctx()).is_empty());
        assert!(CpuCapacity.is_satisfied(&state));
    }
}
