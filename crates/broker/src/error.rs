//! Internal errors produced by the broker's handlers and lifecycle.
//!
//! These are NOT Kafka wire-level error codes (those live in
//! [`crate::codes`]). Conversion from `BrokerError` to a wire code
//! happens at the handler boundary.

use thiserror::Error;

/// Errors produced by the broker's lifecycle and handlers.
///
/// Returned from [`crate::Broker::start`] and propagated up from
/// per-connection serve loops. The `#[non_exhaustive]` attribute lets
/// future variants be added without a breaking change.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BrokerError {
    /// Filesystem I/O failure (binding the listener, opening log dirs).
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// Storage-layer error bubbling up from [`crabka_log`].
    #[error("log: {0}")]
    Log(#[from] crabka_log::LogError),

    /// Wire-protocol decoding or encoding error.
    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    /// The peer sent a `(api_key, version)` the handler table doesn't
    /// know how to serve.
    #[error("unsupported api_key={api_key} version={version}")]
    UnsupportedApi {
        /// The unsupported Kafka API key.
        api_key: i16,
        /// The unsupported version negotiated by the peer.
        version: i16,
    },

    /// A produce request landed on a partition whose writer actor has
    /// exited — typically only seen at shutdown.
    #[error("partition writer for {topic}-{partition} died")]
    PartitionWriterDied {
        /// Topic name of the dead writer.
        topic: String,
        /// Partition index of the dead writer.
        partition: i32,
    },

    /// The broker is shutting down and refuses new work.
    #[error("shutting down")]
    Shutdown,

    /// A failure that occurred during [`crate::Broker::start`] — controller
    /// bring-up, leader election timeout, etc.
    #[error("startup failed: {0}")]
    Startup(String),

    /// A group-coordinator request arrived while the group is in a state
    /// that doesn't allow it (e.g. heartbeat during `PreparingRebalance`).
    #[error("group {group_id} is in state {state:?}, request not allowed")]
    GroupInvalidState {
        /// The affected group id.
        group_id: String,
        /// The current `GroupState` rendered via `Debug`.
        state: String,
    },

    /// The client referenced a `member_id` the coordinator doesn't track
    /// for this group.
    #[error("unknown member {member_id} in group {group_id}")]
    UnknownMember {
        /// The affected group id.
        group_id: String,
        /// The unrecognized member id.
        member_id: String,
    },

    /// The client sent a request bound to a stale generation.
    #[error("group {group_id} generation mismatch: have {current}, got {requested}")]
    GenerationMismatch {
        /// The affected group id.
        group_id: String,
        /// The coordinator's current generation.
        current: i32,
        /// The generation the client supplied.
        requested: i32,
    },

    /// The client's producer epoch is older than the current one registered
    /// for this producer id.
    #[error("producer epoch fenced: pid={producer_id} got {requested}, current {current}")]
    ProducerEpochFenced {
        /// The producer id that was fenced.
        producer_id: i64,
        /// The epoch currently registered for this producer id.
        current: i16,
        /// The epoch the client supplied.
        requested: i16,
    },

    /// A replication-layer failure (fetch from leader failed, truncation
    /// error, etc.). Maps to `UNKNOWN_SERVER_ERROR` on the wire.
    #[error("replication: {0}")]
    Replication(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_unsupported_api() {
        let e = BrokerError::UnsupportedApi {
            api_key: 7,
            version: 9,
        };
        assert!(e.to_string().contains("api_key=7"));
        assert!(e.to_string().contains("version=9"));
    }
}
