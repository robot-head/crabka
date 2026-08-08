//! Internal errors produced by the broker's handlers and lifecycle.
//!
//! These are NOT Kafka wire-level error codes. Those live in
//! [`crate::codes`]. The handler boundary converts a `BrokerError` to a wire
//! code.

use thiserror::Error;

/// Errors produced by the broker's lifecycle and handlers.
///
/// [`crate::Broker::start`] returns these errors, and the per-connection serve
/// loops pass them up. The `#[non_exhaustive]` attribute lets a later release
/// add variants without a breaking change.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BrokerError {
    /// Filesystem I/O failure, for example when the broker binds the listener
    /// or opens log dirs.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// Storage-layer error that comes up from [`crabka_log`].
    #[error("log: {0}")]
    Log(#[from] crabka_log::LogError),

    /// Wire-protocol decoding or encoding error.
    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    /// The peer sent an `(api_key, version)` pair that the handler table
    /// cannot serve.
    #[error("unsupported api_key={api_key} version={version}")]
    UnsupportedApi {
        /// The unsupported Kafka API key.
        api_key: i16,
        /// The unsupported version negotiated by the peer.
        version: i16,
    },

    /// A produce request arrived at a partition whose writer actor has
    /// exited. This normally happens only at shutdown.
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

    /// A failure during [`crate::Broker::start`], such as controller bring-up
    /// or a leader-election timeout.
    #[error("startup failed: {0}")]
    Startup(String),

    /// A group-coordinator request arrived while the group is in a state that
    /// does not allow it, for example a heartbeat during
    /// `PreparingRebalance`.
    #[error("group {group_id} is in state {state:?}, request not allowed")]
    GroupInvalidState {
        /// The affected group id.
        group_id: String,
        /// The current `GroupState` rendered via `Debug`.
        state: String,
    },

    /// The client named a `member_id` that the coordinator does not track for
    /// this group.
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

    #[error("fenced leader epoch (have={have}, current={current})")]
    FencedLeaderEpoch { have: i32, current: i32 },

    #[error("unknown leader epoch ({0})")]
    UnknownLeaderEpoch(i32),

    /// A replication-layer failure, such as a failed fetch from the leader or
    /// a truncation error. It maps to `UNKNOWN_SERVER_ERROR` on the wire.
    #[error("replication: {0}")]
    Replication(String),

    /// A transactional operation failed. It maps to `UNKNOWN_SERVER_ERROR` on
    /// the wire. Handlers choose the specific wire codes.
    #[error("transaction: {0}")]
    Txn(String),

    /// A KIP-932 share-coordinator (persister) operation failed. It maps to
    /// `UNKNOWN_SERVER_ERROR` on the wire. Handlers choose the specific wire
    /// codes.
    #[error("share: {0}")]
    Share(String),

    /// Two listeners share the same `bind_addr`.
    #[error("listener bind conflict: {a} and {b} share bind_addr")]
    ListenerConflict { a: String, b: String },

    /// `inter_broker_listener_name` does not match any listener name.
    #[error("inter_broker_listener_name {name} not in listeners list")]
    InvalidInterBrokerListener { name: String },

    /// `process.roles` was empty. A node must be a `controller`, a `broker`,
    /// or both.
    #[error("process.roles must list at least one role")]
    EmptyRoles,

    /// A non-controller node lists itself in `controller_quorum_voters`.
    #[error("node {node_id} is not a controller but appears in its own controller_quorum_voters")]
    NonControllerIsVoter { node_id: crabka_raft::NodeId },

    /// A SASL listener is declared but `enabled_sasl_mechanisms` is empty.
    #[error("SASL listener {name} declared but enabled_sasl_mechanisms is empty")]
    SaslListenerNoMechanisms { name: String },

    /// `Gssapi` is an enabled SASL mechanism, but the config supplied no
    /// `gssapi` block with a keytab, a service name, and a principal
    /// mapping.
    #[error("GSSAPI is an enabled SASL mechanism but gssapi config is missing")]
    GssapiConfigMissing,

    /// TLS configuration error.
    #[error("tls: {0}")]
    Tls(String),

    /// The broker failed to read or decode the bootstrap records file that
    /// `crabka format --add-scram` wrote.
    #[error("bootstrap file {path:?}: {source}")]
    BootstrapFile {
        /// Path to the file that could not be read or decoded.
        path: std::path::PathBuf,
        /// Underlying I/O or decode error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("invalid leader_imbalance_check_interval = {value}: must be >= 1")]
    InvalidLeaderRebalanceInterval { value: u64 },

    #[error("invalid leader_imbalance_per_broker_percentage = {percent}: must be <= 100")]
    InvalidLeaderRebalanceThreshold { percent: f64 },

    /// Runtime tuning contains an invalid scalar or field relation.
    #[error("invalid runtime configuration: {0}")]
    InvalidRuntimeConfig(String),

    #[error("controlled shutdown did not complete within {0:?}")]
    ShutdownTimeout(std::time::Duration),
}

#[cfg(test)]
mod tests {
    use assert2::assert;

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
