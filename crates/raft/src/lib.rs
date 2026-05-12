//! Metadata Raft quorum for Crabka — openraft adapters + `Controller`.

#![doc(html_root_url = "https://docs.rs/crabka-raft/0.0.0")]

mod error;
mod log_store;
mod network;
mod state_machine;
mod types;
mod wire;

pub use error::RaftError;
pub use types::{AppData, AppDataResponse, Node, NodeId, Raft, TypeConfig};
pub use wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_INSTALL_SNAPSHOT, API_KEY_VOTE, CrabkaAppendEntriesRequest,
    CrabkaAppendEntriesResponse, CrabkaInstallSnapshotRequest, CrabkaInstallSnapshotResponse,
    CrabkaLogEntry, CrabkaVoteRequest, CrabkaVoteResponse, PayloadKind,
};
