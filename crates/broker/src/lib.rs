//! Single-node Apache Kafka-compatible broker (MVP).
//!
//! `crabka-broker` ships a library + binary that an unmodified JVM
//! Kafka client can produce records to and consume from. It is the
//! smallest demonstrable artifact in the Crabka stack.
//!
//! # What this crate does
//!
//! - Accepts TCP connections speaking the Kafka wire protocol.
//! - Handles `ApiVersions`, `Metadata`, `CreateTopics`, `DeleteTopics`,
//!   `Produce`, `Fetch` (with long-poll), `ListOffsets`,
//!   `DescribeConfigs`, and a stub `FindCoordinator`.
//! - Persists records via [`crabka_log`]; one [`Log`](crabka_log::Log)
//!   per (topic, partition) under `<log_dir>/<topic>-<partition>/`.
//! - Reconstructs its in-memory metadata image from the directory
//!   layout on startup.
//!
//! # What this crate doesn't do
//!
//! - Replication, leader election, ISR (slice 8).
//! - `KRaft` metadata quorum (slice 7) — the metadata image is in-memory.
//! - Consumer groups, offset commits, coordinators (slice 5) —
//!   `FindCoordinator` stubs to `COORDINATOR_NOT_AVAILABLE`; consumers
//!   must use `--partition` to bypass groups.
//! - Idempotent / transactional producers (slices 6, 9).
//! - Authentication, TLS, SASL, ACLs (slice 11).
//! - Log compaction, tiered storage, quotas.
//!
//! # Quick start
//!
//! ```no_run
//! use crabka_broker::{Broker, BrokerConfig};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let handle = Broker::start(BrokerConfig::default()).await?;
//! tokio::signal::ctrl_c().await?;
//! handle.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Public surface
//!
//! - [`Broker`] — owns the partition registry, metadata image, and
//!   handler table; constructed by [`Broker::start`].
//! - [`BrokerHandle`] — lifecycle handle returned by
//!   [`Broker::start`]; call [`BrokerHandle::shutdown`] to drain.
//! - [`BrokerConfig`] — listen address, advertised listener, log dir,
//!   broker id, per-log [`LogConfig`](crabka_log::LogConfig).
//! - [`BrokerError`] — error returned by [`Broker::start`].
//!
//! ## Replication (slice 8)
//!
//! `CreateTopics` with `replication_factor > 1` assigns N replicas per
//! partition via round-robin over `MetadataImage::brokers()`. The
//! `replicator_supervisor` subscribes to controller metadata changes
//! and spawns a `replicator` task per partition where this broker is
//! a non-leader replica. Each replicator opens a
//! `crabka_client_core::Client` to its partition's leader and loops
//! on `Fetch` with `replica_id` set, appending every returned
//! `RecordBatch` to the local log.
//!
//! ISR shrink/expand, high-watermark tracking, `acks=all` blocking,
//! `AlterPartition` RPC, leader-election-on-failure, and cross-broker
//! producer routing are deferred — see the slice 8 design spec.
//!
//! ## Transactions (slice 9)
//!
//! Kafka transactions (KIP-98 + full KIP-1319 v2) via a per-broker
//! `txn::coordinator::TxnCoordinator` backed by the `__transaction_state`
//! internal topic (50 partitions, lazily bootstrapped on first
//! `FindCoordinator(TRANSACTION)`). Producers call `init_transactions`
//! / `begin_transaction` / `commit_transaction` / `abort_transaction` /
//! `send_offsets_to_transaction`; consumers set
//! `isolation_level=read_committed` to filter aborted records via the
//! per-segment `.txnindex` and partition-level LSO.
//!
//! Soft-EOS caveat: slice-8 deferrals (HW + acks=all blocking,
//! leader-election-on-failure, KIP-101 leader-epoch) remain deferred.
//! The transactional control plane is correct; a partition-leader
//! crash mid-transaction can lose records the producer believed
//! durably committed. Bulletproof EOS lands when those slice-8
//! follow-ups ship.
//!
//! ## Bulletproof EOS — sub-slice 10a (HW + acks=all)
//!
//! Per-partition High Watermark tracking via `ReplicaState`
//! (lives on `Partition`). The leader maintains each follower's LEO from
//! their Fetch requests and caches HW = `min(LEO over ISR)`. `acks=-1`
//! Produces gate on `Partition::await_hw_at_least` before responding;
//! on timeout the producer gets per-partition
//! `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (code 20). Consumer Fetches
//! (`replica_id == -1`) clamp visible batches and `last_stable_offset`
//! at HW; `read_committed` LSO becomes `min(HW, log.lso())`.
//!
//! Sub-slice 10b will add KIP-101 leader-epoch fencing,
//! leader-election-on-failure, and ISR shrink/expand to close the
//! remaining bulletproof-EOS gap (a leader crash mid-transaction still
//! loses records as of 10a).
//!
//! ## Bulletproof EOS — sub-slice 10b (leader-epoch + election + ISR)
//!
//! KIP-101 leader-epoch fencing tagged onto every appended batch via
//! `Partition::current_leader_epoch`. Per-partition
//! `.leader-epoch-checkpoint` file (Apache Kafka byte-compat) backs the
//! `OffsetForLeaderEpoch` RPC for follower-side truncation on leader
//! change. Leader election runs on the controller:
//! `heartbeat::controller_state::ControllerLivenessState` tracks
//! per-broker `last_heartbeat`; a 1s ticker times out brokers at
//! `heartbeat_timeout_ms` and calls `leader_election::on_broker_dead`
//! which scans partitions of the dead broker, picks the first alive
//! ISR replica, and bumps `leader_epoch`. ISR shrink/expand is
//! leader-driven by `isr_maintenance` — proposes `AlterPartition`
//! whenever a follower's last-fetch time exceeds
//! `replica_lag_time_max_ms`.
//!
//! Together with slice-10a, the bulletproof-EOS promise is complete:
//! `acks=all` produces survive arbitrary single-broker failures with
//! no data loss and no zombie writes.

#![doc(html_root_url = "https://docs.rs/crabka-broker/0.0.0")]

pub mod authorizer;
pub mod bootstrap;
mod broker;
mod codes;
pub mod config;
pub(crate) mod config_keys;
mod coordinator;
mod error;
mod handlers;
pub(crate) mod heartbeat;
pub(crate) mod isr_maintenance;
pub(crate) mod leader_election;
pub mod leader_rebalance;
mod log_dir;
pub mod network;
mod partition;
mod partition_writer;
mod producer_id_manager;
mod producer_state;
pub mod raft_handshake;
pub(crate) mod replica_state;
mod replicator;
mod replicator_supervisor;
mod txn;

pub use broker::{Broker, BrokerHandle};
pub use config::{BootstrapMode, BrokerConfig};
pub use crabka_raft::NodeId;
pub use error::BrokerError;
