//! Slice 35: admin client for the operator. Targets one cluster's
//! controller; plaintext only (slice 36 will add TLS / SASL).
//!
//! Built on `crabka_client_core::Connection`'s typed
//! `send::<R: ProtocolRequest>` so request-version negotiation is
//! automatic via the `ApiVersionTable` populated at connect time.

use std::time::Duration;

use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use thiserror::Error;

pub mod configs;
pub mod topics;

pub use configs::{AlterConfigsOutcome, IncrementalAlterOp, TopicConfigOverrides};
pub use topics::{
    CreatePartitionsOp, CreatePartitionsOutcome, CreateTopicOutcome, CreateTopicSpec,
    DeleteTopicOutcome, TopicMetadata, TopicMetadataEntry,
};

/// Test seam for `AdminClient`. The operator's reconcile only needs
/// dynamic dispatch via this trait; production code wraps a concrete
/// `AdminClient`, while tests substitute a fake.
///
/// Methods take `&mut self` because the underlying `AdminClient`'s
/// `NOT_CONTROLLER` retry path reconnects the inner `Connection` in
/// place, which requires unique access.
#[async_trait::async_trait]
pub trait AdminClientLike: Send {
    async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError>;
    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout_ms: i32,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError>;
    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError>;
    async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout_ms: i32,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError>;
    async fn describe_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<TopicConfigOverrides>, AdminError>;
    async fn incremental_alter_configs(
        &mut self,
        ops: &[IncrementalAlterOp],
    ) -> Result<Vec<AlterConfigsOutcome>, AdminError>;
}

#[async_trait::async_trait]
impl AdminClientLike for AdminClient {
    async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        AdminClient::metadata(self, topics).await
    }
    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout_ms: i32,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        AdminClient::create_topics(self, specs, timeout_ms).await
    }
    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        AdminClient::delete_topics(self, names, timeout_ms).await
    }
    async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout_ms: i32,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        AdminClient::create_partitions(self, ops, timeout_ms).await
    }
    async fn describe_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<TopicConfigOverrides>, AdminError> {
        AdminClient::describe_configs(self, topics).await
    }
    async fn incremental_alter_configs(
        &mut self,
        ops: &[IncrementalAlterOp],
    ) -> Result<Vec<AlterConfigsOutcome>, AdminError> {
        AdminClient::incremental_alter_configs(self, ops).await
    }
}

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("no bootstrap address was reachable: tried {tried}")]
    Connect { tried: usize },
    #[error("controller routing failed after retry")]
    NotControllerExhausted,
    #[error("broker returned error: api={api} code={code} ({name}){detail}",
            detail = .message.as_deref().map(|m| format!(" {m:?}")).unwrap_or_default())]
    Broker {
        api: &'static str,
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    #[error("client-core: {0}")]
    Transport(#[from] ClientError),
    #[error("protocol: {0}")]
    Protocol(String),
}

/// A Kafka-level error attached to a single per-resource outcome.
#[derive(Debug, Clone)]
pub struct KafkaError {
    pub code: i16,
    pub name: &'static str,
    pub message: Option<String>,
}

/// Short-lived admin client targeting one cluster's controller.
/// Plaintext only.
pub struct AdminClient {
    pub(crate) conn: Connection,
}

impl AdminClient {
    /// Try each bootstrap address in order. Each entry is `host:port`;
    /// DNS is resolved via `tokio::net::lookup_host`. First successful
    /// connect wins. Returns `AdminError::Connect { tried }` if none
    /// responded.
    pub async fn connect(bootstrap_addrs: &[String]) -> Result<Self, AdminError> {
        let opts = ConnectionOptions {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            client_id: "crabka-operator".to_string(),
        };
        for host_port in bootstrap_addrs {
            match Self::connect_one(host_port, opts.clone()).await {
                Ok(conn) => return Ok(Self { conn }),
                Err(e) => {
                    tracing::debug!(
                        target: "crabka_client_admin",
                        addr = %host_port,
                        error = %e,
                        "bootstrap connect failed",
                    );
                }
            }
        }
        Err(AdminError::Connect {
            tried: bootstrap_addrs.len(),
        })
    }

    async fn connect_one(
        host_port: &str,
        opts: ConnectionOptions,
    ) -> Result<Connection, AdminError> {
        let mut addrs = tokio::net::lookup_host(host_port)
            .await
            .map_err(|e| AdminError::Protocol(format!("DNS lookup {host_port}: {e}")))?;
        let addr = addrs
            .next()
            .ok_or_else(|| AdminError::Protocol(format!("no addresses for {host_port}")))?;
        Connection::connect(addr, opts)
            .await
            .map_err(AdminError::from)
    }

    /// Replace the underlying connection. Used internally by the
    /// `NOT_CONTROLLER` retry path to reconnect to the current controller.
    pub(crate) async fn reconnect(&mut self, host_port: &str) -> Result<(), AdminError> {
        let opts = ConnectionOptions {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            client_id: "crabka-operator".to_string(),
        };
        self.conn = Self::connect_one(host_port, opts).await?;
        Ok(())
    }
}

/// Kafka error code: the broker is not the controller (KIP-129). The
/// admin client refreshes its controller endpoint and retries once.
pub(crate) const NOT_CONTROLLER: i16 = 41;

/// Map a Kafka error code into a static name string for human-friendly
/// `AdminError::Broker` formatting. Only the codes we actually surface
/// today; unknown codes serialize as `"UNKNOWN"`.
pub(crate) fn kafka_error_name(code: i16) -> &'static str {
    match code {
        0 => "NONE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        7 => "REQUEST_TIMED_OUT",
        17 => "INVALID_TOPIC_EXCEPTION",
        19 => "NOT_ENOUGH_REPLICAS",
        36 => "TOPIC_ALREADY_EXISTS",
        37 => "INVALID_PARTITIONS",
        38 => "INVALID_REPLICATION_FACTOR",
        39 => "INVALID_REPLICA_ASSIGNMENT",
        40 => "INVALID_CONFIG",
        41 => "NOT_CONTROLLER",
        87 => "REASSIGNMENT_IN_PROGRESS",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_error_name_known_codes() {
        assert_eq!(kafka_error_name(0), "NONE");
        assert_eq!(kafka_error_name(36), "TOPIC_ALREADY_EXISTS");
        assert_eq!(kafka_error_name(41), "NOT_CONTROLLER");
    }

    #[test]
    fn kafka_error_name_unknown_returns_unknown() {
        assert_eq!(kafka_error_name(9999), "UNKNOWN");
    }
}
