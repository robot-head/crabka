//! Timestamp-oracle seams for range-0 timestamp transactions.

pub mod client;
pub mod oracle;

pub use self::{
    client::{BatchedTsoClient, TsoRpc},
    oracle::{
        EpochHeartbeat, GrantLease, HeartbeatVerdict, MAX_TS_KEY, MemoryTsoHorizon, TsoError,
        TsoHorizonCommitter, TsoOracle, TsoTimestamp,
    },
};
