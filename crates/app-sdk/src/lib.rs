//! Crabka application SDK for Rust.
//!
//! This crate implements the cross-language application SDK contract used by
//! serverless app code. It deliberately sits beside, not above, the native
//! Kafka-shaped `crabka-client-*` crates: use this SDK for the portable app
//! surface, and use the native crates for Kafka protocol administration,
//! producing, consuming, and stream-processing internals.

pub mod client;
pub mod connect_client;
pub mod error;
pub mod messaging;
pub mod stubs;

pub use self::{
    client::{CrabkaClient, CrabkaClientBuilder},
    error::CrabkaError,
    messaging::{
        CloudEvent, Filter, Inbound, MessageStream, MessagingClient, PublishOptions, RecordResult,
    },
    stubs::{
        AcquireOptions, AuthClient, BlobClient, DatabaseClient, QueueAckEntry, QueueAckResult,
        QueueAckType, QueueAcquireResult, QueueMessage, QueueRenewEntry, QueuesClient,
    },
};

/// Generated gateway protobuf messages.
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.gateway.v1.rs"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_gateway_messages_are_visible() {
        let request = crate::pb::SendRequest::default();
        assert!(request.records.is_empty());
    }
}
