//! Metadata Raft quorum for Crabka.
//!
//! `crabka-raft` runs a hand-rolled KIP-595 `KRaft` consensus engine (the
//! [`kraft::KraftController`]) over Crabka's storage ([`crabka_log`]) and
//! transport ([`crabka_client_core`]). The public entry point is
//! [`Controller::start`], which spawns the engine, opens a TCP listener serving
//! the real KIP-595 RPCs (Fetch=1, Vote=52, BeginQuorumEpoch=53,
//! EndQuorumEpoch=54) plus the Crabka-private observer/forward RPCs, and returns
//! a [`ControllerHandle`] for submitting metadata changes and reading the
//! current [`crabka_metadata::MetadataImage`].
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use crabka_metadata::{MetadataRecord, TopicRecord};
//! use crabka_raft::{Controller, ControllerConfig};
//! use uuid::Uuid;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
//! let controller = Controller::start(cfg).await?;
//!
//! controller.submit_change(vec![
//!     MetadataRecord::V1Topic(TopicRecord {
//!         name: "my-topic".into(),
//!         topic_id: Uuid::new_v4(),
//!         partitions: 3,
//!         replication_factor: 1,
//!     }),
//! ]).await?;
//!
//! assert!(controller.current_image().topic("my-topic").is_some());
//! controller.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Current boundaries
//!
//! - Cross-node snapshot transfer / `FetchSnapshot` catch-up is limited to
//!   the broker-facing checkpoint read path.
//! - Dynamic voter membership changes return [`RaftError::Unsupported`];
//!   voter membership is static for a controller process.
//! - Mixed JVM+Crabka controller quorums are not supported.

#![doc(html_root_url = "https://docs.rs/crabka-raft/0.0.0")]

mod config;
mod controller;
mod error;
pub mod handshake;
pub mod kraft;
mod network;
pub mod reconfig;
mod server;
mod snapshot;
mod types;
mod wire;

pub use config::{BootstrapMode, ControllerConfig};
pub use controller::{Controller, ControllerHandle, QuorumState, SnapshotRange, SnapshotSlice};
pub use error::RaftError;
pub use handshake::{DuplexStream, RaftHandshakeError, RaftListenerHandshake};
pub use kraft::MetadataFetchSlice;
pub use network::{OutboundDialer, PlaintextDialer};
pub use reconfig::{AddVoter, ReconfigOutcome, RemoveVoter, UpdateVoter};
pub use types::{AppData, AppDataResponse, Node, NodeId};
pub use wire::{
    API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, CrabkaMetadataFetchRequest,
    CrabkaMetadataFetchResponse, CrabkaSubmitChangeRequest, CrabkaSubmitChangeResponse,
};
