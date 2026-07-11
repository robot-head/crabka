//! Crabka rebalancer — Cruise-Control-equivalent partition placement
//! advisor and executor.
//!
//! The crate ingests broker/topic metrics, builds a cluster model, evaluates
//! goal plugins, and emits executable partition-reassignment plans. The
//! Connect-RPC API exposes the same analysis and execution path used by the
//! Kubernetes operator.
//!
//! ## Optimization workflow
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use crabka_rebalancer::{
//!     capacity::BrokerCapacities,
//!     goals::{GoalContext, leader_distribution::LeaderDistribution},
//!     model::{BrokerView, ClusterState, PartitionView},
//!     optimizer,
//!     scraper::UsageStore,
//! };
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let state = ClusterState {
//!     cluster_id: Some("cluster-a".into()),
//!     snapshot_at_ms: 1_713_000_000_000,
//!     brokers: vec![
//!         BrokerView {
//!             id: 1,
//!             host: "b1".into(),
//!             port: 9092,
//!             rack: None,
//!         },
//!         BrokerView {
//!             id: 2,
//!             host: "b2".into(),
//!             port: 9092,
//!             rack: None,
//!         },
//!     ],
//!     partitions: vec![PartitionView {
//!         topic: "orders".into(),
//!         partition: 0,
//!         replicas: vec![1, 2],
//!         leader: 1,
//!         isr: vec![1, 2],
//!     }],
//!     in_flight_reassignments: Vec::new(),
//! };
//! let ctx = GoalContext {
//!     imbalance_threshold_pct: 10,
//!     max_movements_per_proposal: 100,
//!     min_topic_leaders_per_broker: 0,
//!     broker_capacities: Arc::new(BrokerCapacities::default()),
//!     broker_usages: Arc::new(UsageStore::default()),
//! };
//! let goal = LeaderDistribution;
//! let out = optimizer::optimize(&state, &[&goal], &ctx)?;
//! println!("{} partition movements", out.proposal.movements.len());
//! # Ok(())
//! # }
//! ```

/// Generated protobuf + Connect server stubs. The actual content lives
/// in `OUT_DIR/crabka.rebalancer.v1.rs` and is produced by `build.rs`.
///
/// Pedantic lints are silenced here because the include is verbatim
/// codegen output; we cannot retrofit `#[must_use]` annotations or
/// shorter helper functions without forking the upstream codegen.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.rebalancer.v1.rs"));
}

pub mod api;
pub mod capacity;
pub mod detector;
pub mod executor;
pub mod goals;
pub mod health;
pub mod ingest;
pub mod metrics;
pub mod model;
pub mod optimizer;
pub mod scraper;
pub mod state_topic;

pub(crate) mod time;
