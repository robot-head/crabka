//! Runtime configuration for the registry service.

/// Resolved configuration for a running registry node.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// `host:port[,host:port...]` bootstrap addresses for the Crabka broker.
    pub bootstrap: String,
    /// Name of the backing compacted topic. Confluent default: `_schemas`.
    pub schemas_topic: String,
    /// Replication factor for `_schemas` when auto-created.
    pub schemas_topic_rf: i32,
    /// Client id used for the producer/reader connections.
    pub client_id: String,
}
