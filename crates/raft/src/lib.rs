//! Metadata Raft quorum for Crabka.
//!
//! `crabka-raft` runs a hand-rolled KIP-595 `KRaft` consensus engine, the
//! [`kraft::KraftController`], over Crabka's storage ([`crabka_log`]) and
//! transport ([`crabka_client_core`]). The public entry point is
//! [`Controller::start`]. It spawns the engine and opens a TCP listener. That
//! listener serves the real KIP-595 RPCs (Fetch=1, Vote=52,
//! BeginQuorumEpoch=53, EndQuorumEpoch=54) and the Crabka-private observer and
//! forward RPCs. [`Controller::start`] returns a [`ControllerHandle`], which
//! submits metadata changes and reads the current
//! [`crabka_metadata::MetadataImage`].
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use crabka_metadata::{MetadataRecord, TopicRecord};
//! use crabka_raft::{Controller, ControllerConfig, NodeId};
//! use uuid::Uuid;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
//! let controller = Controller::start(cfg).await?;
//!
//! controller
//!     .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
//!         name: "my-topic".into(),
//!         topic_id: Uuid::new_v4(),
//!         partitions: 3,
//!         replication_factor: 1,
//!     })])
//!     .await?;
//!
//! assert!(controller.current_image().topic("my-topic").is_some());
//! controller.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Capabilities and boundaries
//!
//! The controller persists and recovers `KRaft` metadata records, and it serves
//! and installs KIP-630 snapshots through `FetchSnapshot`. It also publishes
//! the current metadata image to broker tasks and exposes Crabka-private submit
//! and fetch RPCs for broker and observer integration.
//!
//! KIP-853-style observer bootstrap and auto-join are wired through the broker
//! and controller configuration. The older handle-level `add_learner` and
//! `change_membership` compatibility methods still return
//! [`RaftError::Unsupported`]. Mixed JVM and Crabka controller quorums are
//! outside this crate's compatibility target.

#![doc(html_root_url = "https://docs.rs/crabka-raft/0.3.9")]

mod config;
mod controller;
mod error;
pub mod handshake;
pub mod kraft;
mod network;
pub mod reconfig;
/// The deterministic `KRaft` failure-scenario simulator with trace recording,
/// re-exported from the leaf [`crabka_kraft_core::sim`] module. `crabka-docgen`
/// runs [`scenarios::scenarios`] in-process to render the failure-scenario
/// slideshow.
#[cfg(feature = "scenarios")]
pub use crabka_kraft_core::sim as scenarios;
mod server;
mod snapshot;
mod types;
mod wire;

pub use config::{
    BootstrapMode, ControllerConfig, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
    MetadataRaftFetchMax, RaftShardRouter, ShardRouteFuture,
};
pub use controller::{
    Controller, ControllerHandle, QuorumState, SnapshotRange, SnapshotSlice, metadata_log_nonempty,
};
pub use error::RaftError;
pub use handshake::{DuplexStream, RaftHandshakeError, RaftListenerHandshake};
pub use kraft::MetadataFetchSlice;
pub use network::{OutboundDialer, PlaintextDialer};
pub use reconfig::{AddVoter, ReconfigOutcome, RemoveVoter, UpdateVoter};
pub use types::{AppData, AppDataResponse, Node, NodeId, OffsetReservation, SubmitChangeResult};
pub use wire::{
    API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, CrabkaMetadataFetchRequest,
    CrabkaMetadataFetchResponse, CrabkaSubmitChangeRequest, CrabkaSubmitChangeResponse,
};
