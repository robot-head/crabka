//! Schema Registry primary election (cp-exact `"sr"` Kafka group). A node joins
//! the group; the leader selects the primary and broadcasts it; every node
//! publishes its `PrimaryState` for the forwarding middleware.

pub mod client;
pub mod protocol;

/// Who the primary is, from this node's point of view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryState {
    pub is_primary: bool,
    pub primary_url: Option<String>,
}
