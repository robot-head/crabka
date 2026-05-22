//! Crabka Kubernetes operator library.
//!
//! The binary entry point is `src/main.rs`; this library exposes the
//! reusable pieces (controllers, CRD types, telemetry, leader election)
//! so they can be unit-tested without spinning up the binary.

pub mod config;
pub mod context;
pub mod controller;
pub mod crd;
pub mod gen_crds;
pub mod health;
pub mod leader_election;
pub mod rebalancer_client;
pub mod run;
pub mod telemetry;
