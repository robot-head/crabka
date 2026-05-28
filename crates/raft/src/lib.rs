//! Metadata Raft quorum for Crabka.
//!
//! `crabka-raft` adapts [openraft][openraft] to Crabka's storage
//! ([`crabka_log`]) and transport ([`crabka_client_core`]). The public
//! entry point is [`Controller::start`], which spawns an openraft node,
//! opens a TCP listener for Crabka-private Raft RPCs (api keys 1000-
//! 1002), and returns a [`ControllerHandle`] for submitting metadata
//! changes and reading the current [`crabka_metadata::MetadataImage`].
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
//! ## Out of scope
//!
//! - Snapshots / `InstallSnapshot` (handler is a stub).
//! - Dynamic voter membership changes.
//! - `KRaft` wire compatibility (api keys 52-55, `KRaft` Fetch).
//!
//! [openraft]: https://github.com/databendlabs/openraft

#![doc(html_root_url = "https://docs.rs/crabka-raft/0.0.0")]

mod config;
mod controller;
mod error;
pub mod handshake;
mod log_store;
mod network;
mod server;
mod state_machine;
mod types;
mod wire;

pub use config::{BootstrapMode, ControllerConfig};
pub use controller::{Controller, ControllerHandle, QuorumState};
pub use error::RaftError;
pub use handshake::{DuplexStream, RaftHandshakeError, RaftListenerHandshake};
pub use network::OutboundDialer;
pub use types::{AppData, AppDataResponse, Node, NodeId, Raft, TypeConfig};
pub use wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_INSTALL_SNAPSHOT, API_KEY_VOTE, CrabkaAppendEntriesRequest,
    CrabkaAppendEntriesResponse, CrabkaInstallSnapshotRequest, CrabkaInstallSnapshotResponse,
    CrabkaLogEntry, CrabkaVoteRequest, CrabkaVoteResponse, PayloadKind,
};
