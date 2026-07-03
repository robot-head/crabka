//! Crabka Kubernetes operator library.
//!
//! The binary entry point is `src/main.rs`; this library exposes the
//! reusable pieces (controllers, CRD types, telemetry, leader election)
//! so they can be unit-tested without spinning up the binary.
//!
//! ## Runtime config scope
//!
//! ```rust
//! use crabka_operator::config::OperatorConfig;
//!
//! # fn example(mut config: OperatorConfig) {
//! config.watch_namespaces = vec!["kafka-a".into(), "kafka-b".into()];
//! assert_eq!(config.watched().unwrap().len(), 2);
//!
//! config.watch_namespaces.clear();
//! assert!(config.watched().is_none());
//! # }
//! ```

pub mod config;
pub mod context;
pub mod controller;
pub mod crd;
pub mod gen_crds;
pub mod health;
pub mod ids;
pub mod leader_election;
pub mod rebalancer_client;
pub mod run;
pub mod telemetry;
pub mod version;
