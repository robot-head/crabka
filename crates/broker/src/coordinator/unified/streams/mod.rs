//! KIP-1071 Streams rebalance-protocol subsystem.
//!
//! This subsystem does broker-side task assignment for Kafka Streams groups.
//! It ingests the topology, manages internal topics, assigns active, standby,
//! and warmup tasks, and runs the heartbeat epoch sequence.
//!
//! In shape it mirrors the KIP-932 share-group subsystem, `super::share`. In
//! reconciliation mechanics it mirrors the KIP-848 next-gen consumer
//! coordinator, `super`. But it assigns *tasks* `(subtopology, partition)`
//! across active, standby, and warmup roles rather than partitions.
pub mod actor;
pub mod assignor;
pub mod config;
pub(crate) mod migration;
pub mod persistence;
pub mod state;
pub mod topology;
