# crabka-rebalancer

[![Crates.io](https://img.shields.io/crates/v/crabka-rebalancer.svg)](https://crates.io/crates/crabka-rebalancer)
[![Docs.rs](https://docs.rs/crabka-rebalancer/badge.svg)](https://docs.rs/crabka-rebalancer)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Cruise-Control-equivalent partition rebalancer for Crabka clusters.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-rebalancer
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Evaluate a leader-distribution goal against an in-memory cluster model:

```rust,no_run
use std::sync::Arc;
use crabka_rebalancer::capacity::BrokerCapacities;
use crabka_rebalancer::goals::{GoalContext, leader_distribution::LeaderDistribution};
use crabka_rebalancer::model::{BrokerView, ClusterState, PartitionView};
use crabka_rebalancer::optimizer;
use crabka_rebalancer::scraper::UsageStore;
use crabka_units::percent;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let state = ClusterState {
    cluster_id: Some("cluster-a".into()),
    snapshot_at_ms: 1_713_000_000_000,
    brokers: vec![
        BrokerView { id: 1, host: "b1".into(), port: 9092, rack: None },
        BrokerView { id: 2, host: "b2".into(), port: 9092, rack: None },
    ],
    partitions: vec![PartitionView {
        topic: "orders".into(),
        partition: 0,
        replicas: vec![1, 2],
        leader: 1,
        isr: vec![1, 2],
    }],
    in_flight_reassignments: Vec::new(),
};
let ctx = GoalContext {
    imbalance_threshold: percent(10),
    max_movements_per_proposal: 100,
    min_topic_leaders_per_broker: 0,
    broker_capacities: Arc::new(BrokerCapacities::default()),
    broker_usages: Arc::new(UsageStore::default()),
};
let goal = LeaderDistribution;
let out = optimizer::optimize(&state, &[&goal], &ctx)?;
println!("{} proposed movements", out.proposal.movements.len());
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-rebalancer](https://docs.rs/crabka-rebalancer). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
