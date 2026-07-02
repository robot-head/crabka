//! Subscribe-style consumer client for Apache Kafka in Rust.
//!
//! Builds on `crabka-client-core` for transport; adds the classic
//! consumer-group lifecycle (`JoinGroup` → `SyncGroup` → `Heartbeat` →
//! `Fetch` → `OffsetCommit` → `LeaveGroup`) and a built-in heartbeat
//! task.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use crabka_client_consumer::{AutoOffsetReset, Consumer};
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
//! ## Share-group consumption
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use crabka_client_consumer::{ShareAckMode, ShareAckType, ShareConsumer};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut consumer = ShareConsumer::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("share-workers")
//!     .subscribe(["jobs".to_string()])
//!     .ack_mode(ShareAckMode::Explicit)
//!     .build()
//!     .await?;
//!
//! let records = consumer.poll(Duration::from_secs(1)).await?;
//! for record in &records {
//!     consumer.acknowledge(record, ShareAckType::Accept)?;
//! }
//! consumer.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Capabilities and boundaries
//!
//! This crate owns consumer-facing semantics: classic group membership,
//! assignment, fetch/poll, offset commit, cooperative shutdown, and KIP-932
//! share-group consumption. It intentionally does not duplicate admin-client
//! surfaces such as `DescribeGroups`/`ListGroups`, and manual partition fetches
//! remain available through the lower-level helpers in `crabka-client-core`.
//! Transactional consume-process-produce workflows use this crate's
//! [`ConsumerGroupMetadata`] together with `crabka-client-producer`'s
//! `send_offsets_to_transaction` support.
//!
//! ## Cargo features
//!
//! None for now.

#![doc(html_root_url = "https://docs.rs/crabka-client-consumer/0.3.8")]

mod assignor;
mod builder;
mod commit;
mod consumer;
mod coordinator;
mod error;
mod group_metadata;
#[cfg(test)]
mod lock_order_model;
mod offset_wire;
mod poll;
mod position;
mod seek;
mod share;
mod validate;

pub use assignor::Assignor;
pub use builder::{AutoOffsetReset, IsolationLevel};
pub use consumer::{Consumer, ConsumerRecord, Header};
pub use error::ConsumerError;
pub use group_metadata::ConsumerGroupMetadata;
pub use share::{ShareAckMode, ShareAckType, ShareConsumer, ShareConsumerRecord};
