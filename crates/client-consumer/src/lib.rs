//! Subscribe-style consumer client for Apache Kafka in Rust.
//!
//! Builds on `crabka-client-core` for transport; adds the classic
//! consumer-group lifecycle (`JoinGroup` → `SyncGroup` → `Heartbeat` →
//! `Fetch` → `OffsetCommit` → `LeaveGroup`) and a built-in heartbeat
//! task.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-consumer-groups-design.md`.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use crabka_client_consumer::{Consumer, AutoOffsetReset};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut consumer = Consumer::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("my-group")
//!     .client_id("my-app")
//!     .auto_offset_reset(AutoOffsetReset::Earliest)
//!     .subscribe(["my-topic".to_string()])
//!     .build()
//!     .await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_millis(500)).await?;
//!     for _r in records {
//!         // ... handle r ...
//!     }
//!     consumer.commit_sync().await?;
//! }
//! # }
//! ```
//!
//! ## Out of scope
//!
//! - `assign()` (manual partition consumption) — use `crabka-client-core`
//!   directly.
//! - Admin RPCs (`DescribeGroups`, `ListGroups`) — slice 10.
//! - KIP-848 / cooperative-sticky rebalance — slice 5b.
//! - Transactional consumers (`isolation.level=read_committed`) — slice 9.
//!
//! ## Cargo features
//!
//! None for now.

#![doc(html_root_url = "https://docs.rs/crabka-client-consumer/0.0.0")]

mod assignor;
mod builder;
mod commit;
mod consumer;
mod error;
mod heartbeat;
mod poll;

pub use builder::AutoOffsetReset;
pub use consumer::{Consumer, ConsumerRecord};
pub use error::ConsumerError;
