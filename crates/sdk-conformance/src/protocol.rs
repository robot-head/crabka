//! JSON-lines protocol shared by the conformance harness and SDK adapters.

use serde::{Deserialize, Serialize};

/// Major version for the v1 SDK conformance contract.
pub const CONTRACT_MAJOR: u16 = 1;

/// Minor version for adapters that only implement the original v1.0 contract.
pub const CONTRACT_MINOR_V1_0: u16 = 0;

/// Minor version for adapters that implement queue RPC shapes.
pub const CONTRACT_MINOR_QUEUE_RPC: u16 = 1;

/// A command sent from the conformance harness to a language adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Negotiate the contract major and adapter language.
    Hello,
    /// Configure the SDK endpoint and optional bearer token.
    Configure {
        /// Gateway endpoint URL.
        endpoint: String,
        /// Optional bearer token to forward through the SDK.
        bearer: Option<String>,
    },
    /// Publish a raw record.
    Publish {
        /// Topic name.
        topic: String,
        /// Base64-encoded record value.
        value_b64: String,
        /// Binary headers in publish order.
        headers: Vec<Header>,
    },
    /// Publish a `CloudEvent` in binary mode.
    PublishEvent {
        /// Topic name.
        topic: String,
        /// `CloudEvent` payload and attributes.
        event: CloudEvent,
    },
    /// Open or replace a subscription.
    Subscribe {
        /// Topics to subscribe to.
        topics: Vec<String>,
        /// Consumer group id.
        group: String,
        /// Optional structured equality filter.
        filter: Option<Filter>,
    },
    /// Pull the next message from the active subscription.
    NextMessage {
        /// Maximum wait time in milliseconds.
        timeout_ms: u64,
    },
    /// Queue acquire stub command.
    QueueAcquire {
        /// Topic/queue name.
        topic: String,
        /// Worker group id.
        group: String,
        /// Maximum number of messages.
        max: u32,
        /// Lock duration in milliseconds.
        lock_duration_ms: u64,
    },
    /// Queue ack stub command kept in the v1.0 stub-vector group.
    QueueAck {
        /// Message id to ack.
        message_id: String,
    },
    /// Queue acknowledge command for the v1.1 queue RPC contract group.
    QueueAcknowledge {
        /// Session id returned by queue acquire.
        session_id: String,
        /// Entries to acknowledge.
        entries: Vec<QueueAckEntry>,
    },
    /// Queue renew command for the v1.1 queue RPC contract group.
    QueueRenew {
        /// Session id returned by queue acquire.
        session_id: String,
        /// Entries to renew.
        entries: Vec<QueueRenewEntry>,
    },
    /// Database connect stub command.
    DbConnect {
        /// Database name.
        name: String,
    },
    /// Auth sign-in placeholder command.
    AuthSignIn {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Blob put stub command.
    BlobPut {
        /// Blob key.
        key: String,
        /// Base64-encoded blob body.
        value_b64: String,
    },
    /// Blob get stub command.
    BlobGet {
        /// Blob key.
        key: String,
    },
}

/// A response sent by an adapter for a single [`Command`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// Contract negotiation response.
    Hello {
        /// Adapter contract major.
        contract_major: u16,
        /// Adapter contract minor.
        contract_minor: u16,
        /// Adapter language name.
        language: String,
    },
    /// Successful command response.
    Ok(serde_json::Value),
    /// Message delivered by a subscription.
    Message(Message),
    /// Adapter or SDK error.
    Error(AdapterError),
}

/// Wire error taxonomy shared across language SDKs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// Transport or endpoint reachability failure.
    Transport,
    /// Authentication failed or credentials were absent.
    Unauthenticated,
    /// Caller supplied an invalid argument.
    InvalidArgument,
    /// Target resource was not found.
    NotFound,
    /// Server-side failure outside the narrower classes.
    ServerError,
    /// Module is intentionally gated on later work.
    Unimplemented,
}

/// Error body returned by adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterError {
    /// Taxonomy kind.
    pub kind: ErrorKind,
    /// Optional SDK module name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// Optional plan/spec slug gating an unimplemented module.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gated_on: Option<String>,
    /// Optional diagnostic message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl AdapterError {
    /// Build a pinned unimplemented-module error.
    #[must_use]
    pub fn unimplemented(module: &str, gated_on: &str) -> Self {
        Self {
            kind: ErrorKind::Unimplemented,
            module: Some(module.to_string()),
            gated_on: Some(gated_on.to_string()),
            message: None,
        }
    }

    /// Build an error with only a kind and message.
    #[must_use]
    pub fn with_message(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            module: None,
            gated_on: None,
            message: Some(message.into()),
        }
    }
}

/// Binary record header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    /// Header name.
    pub name: String,
    /// Base64-encoded header value, or null for a Kafka null header value.
    pub value_b64: Option<String>,
}

/// Message response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Base64-encoded record value.
    pub value_b64: String,
    /// Binary headers in delivery order.
    pub headers: Vec<Header>,
}

/// Queue acknowledgement type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueAckType {
    /// Accept and remove the queue message.
    Accept,
    /// Release the queue message for redelivery.
    Release,
    /// Reject the queue message.
    Reject,
}

/// Queue acknowledgement entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckEntry {
    /// Message id returned by queue acquire.
    pub message_id: String,
    /// Ack verdict.
    pub ack_type: QueueAckType,
}

/// Queue renew entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRenewEntry {
    /// Message id returned by queue acquire.
    pub message_id: String,
}

/// Minimal `CloudEvents` v1 binary-mode payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudEvent {
    /// Event id.
    pub id: String,
    /// Event source.
    pub source: String,
    /// Event type.
    #[serde(rename = "type")]
    pub type_: String,
    /// `CloudEvents` specversion.
    pub specversion: String,
    /// Optional data content type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datacontenttype: Option<String>,
    /// Base64-encoded event data.
    pub data_b64: String,
}

/// Structured subscription filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    /// JSON field path. v1 mock vectors support `$.field`.
    pub path: String,
    /// Predicate operation.
    pub op: FilterOp,
    /// Expected scalar JSON value.
    pub value: serde_json::Value,
}

/// Supported filter operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    /// Equality comparison.
    Equals,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_round_trips() {
        let cmd = Command::Publish {
            topic: "t".into(),
            value_b64: "aGk=".into(),
            headers: vec![],
        };
        let line = serde_json::to_string(&cmd).unwrap();
        assert!(serde_json::from_str::<Command>(&line).unwrap() == cmd);

        let err: Response = serde_json::from_str(
            r#"{"error":{"kind":"unimplemented","module":"queues","gated_on":"gateway-sharegroup-rpc"}}"#,
        )
        .unwrap();
        assert2::assert!(let Response::Error(e) = err);
        assert!(e.kind == ErrorKind::Unimplemented);
        assert!(e.gated_on.as_deref() == Some("gateway-sharegroup-rpc"));
    }
}
