//! KIP-1071 Streams rebalance-protocol subsystem. Broker-side task assignment
//! for Kafka Streams groups: topology ingestion, internal-topic management,
//! active/standby/warmup task assignment, and the heartbeat epoch dance.
//!
//! Mirrors the KIP-932 share-group subsystem (`super::share`) in shape and the
//! KIP-848 next-gen consumer coordinator (`super`) in reconciliation mechanics,
//! but assigns *tasks* `(subtopology, partition)` across active/standby/warmup
//! roles rather than partitions.
pub mod actor;
pub mod assignor;
pub mod config;
pub(crate) mod migration;
pub mod persistence;
pub mod state;
pub mod topology;
