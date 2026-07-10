//! Error types for the Gres control plane.

use thiserror::Error;

/// Errors returned by registry validation, serialization, and Kafka IO.
#[derive(Debug, Error)]
pub enum ControlError {
    /// A tenant identifier or field failed boundary parsing.
    #[error("invalid {field}: {reason}")]
    InvalidField {
        /// Field name that failed validation.
        field: &'static str,
        /// Human-readable reason.
        reason: String,
    },
    /// Registry key bytes were not in the supported greenfield format.
    #[error("invalid registry key: {0}")]
    InvalidKey(String),
    /// Registry value bytes were not in the supported greenfield format.
    #[error("invalid registry value: {0}")]
    InvalidValue(String),
    /// A tenant lifecycle transition was not allowed by the registry state machine.
    #[error("invalid tenant lifecycle transition from {from} to {to}")]
    InvalidLifecycleTransition {
        /// Current lifecycle state.
        from: crate::record::TenantState,
        /// Requested lifecycle state.
        to: crate::record::TenantState,
    },
    /// The registry topic could not be created or described.
    #[error("admin client error: {0}")]
    Admin(#[from] crabka_client_admin::AdminError),
    /// Produce failed.
    #[error("producer error: {0}")]
    Producer(#[from] crabka_client_producer::ProducerError),
    /// The registry reader failed to fetch from Kafka.
    #[error("client error: {0}")]
    Client(#[from] crabka_client_core::ClientError),
    /// JSON serialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// TOML serialization failed.
    #[error("toml error: {0}")]
    Toml(#[from] toml::ser::Error),
    /// A one-shot producer acknowledgement was dropped.
    #[error("producer dropped acknowledgement")]
    ProducerAckDropped,
    /// The Kafka broker returned an unsuccessful topic outcome.
    #[error("topic {topic} creation failed with {name} ({code})")]
    TopicCreateFailed {
        /// Topic name.
        topic: String,
        /// Kafka error name.
        name: &'static str,
        /// Kafka error code.
        code: i16,
    },
    /// The compacted registry topic was still absent after create/describe.
    #[error("registry topic {0} not found after create")]
    TopicMissing(String),
    /// The registry backend cannot make the requested mutation safely.
    #[error("unsupported registry mutation {mutation}: {reason}")]
    UnsupportedRegistryMutation {
        /// Mutation that was rejected.
        mutation: &'static str,
        /// Why this backend cannot safely execute it.
        reason: &'static str,
    },
    /// A versioned registry mutation observed a different tenant version.
    #[error("registry version conflict for tenant {tenant}: expected {expected}, found {actual}")]
    RegistryVersionConflict {
        /// Tenant whose record changed concurrently.
        tenant: crate::record::TenantName,
        /// Caller-observed version required by the mutation.
        expected: u64,
        /// Latest version in the registry.
        actual: u64,
    },
    /// A versioned registry mutation cannot be reconciled with the latest layout.
    #[error("registry layout conflict for tenant {tenant}: {reason}")]
    RegistryLayoutConflict {
        /// Tenant whose layout conflicted with the requested mutation.
        tenant: crate::record::TenantName,
        /// Human-readable conflict reason.
        reason: String,
    },
}

impl ControlError {
    pub(crate) fn invalid_field(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidField {
            field,
            reason: reason.into(),
        }
    }
}
