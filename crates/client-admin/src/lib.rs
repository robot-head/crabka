//! Admin client for Crabka operators and control-plane services.
//!
//! The client targets the active controller and retries selected RPCs on a
//! refreshed controller connection when the broker returns `NOT_CONTROLLER`.
//! It supports plaintext by default and the same client-side TLS/SASL security
//! surface as [`crabka_client_core`] through [`AdminClient::connect_secured`].
//!
//! The client is built on `crabka_client_core::Connection`'s typed
//! `send::<R: ProtocolRequest>`, so request-version negotiation is automatic
//! through the `ApiVersionTable` that connect time populates. The public
//! modules cover topic CRUD, partition expansion, config changes, SCRAM user
//! credentials, ACLs, quotas, delegation tokens, and log-dir inspection.

use std::{any::Any, sync::Mutex};

use crabka_client_core::{
    ClientError, Connection, ConnectionOptions, MetadataRecoveryRebootstrapTrigger,
    MetadataRecoveryStrategy, ProtocolRequest as _,
};
use crabka_units::{Time, convert::TimeExt as _, secs};
use thiserror::Error;

pub mod configs;
pub mod delegation_tokens;
pub mod features;
pub mod groups;
pub mod log_dirs;
pub mod quorum;
pub mod quotas;
pub mod topics;
pub mod transactions;
pub mod users;

/// Result of applying a `metadata.version` feature update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataVersionUpdate {
    pub level: i16,
}

pub use configs::{AlterConfigsOutcome, IncrementalAlterOp, TopicConfigOverrides};
pub use log_dirs::{AlterReplicaLogDirOutcome, LogDirInfo, LogDirPartitionInfo, LogDirTopicInfo};
pub use quorum::{MetadataQuorum, QuorumReplica};
pub use quotas::{QuotaOp, UserQuotaConfig, diff_user_quotas};
pub use topics::{
    CreatePartitionsOp, CreatePartitionsOutcome, CreateTopicOutcome, CreateTopicSpec,
    DeleteRecordsOp, DeleteRecordsOutcome, DeleteTopicOutcome, TopicMetadata, TopicMetadataEntry,
    TopicReplicationStatus,
};
pub use users::{
    AclEntry, AclEntryFilter, AclOperation, CreateAclOutcome, DEFAULT_SCRAM_ITERATIONS,
    DeleteAclFilterOutcome, MAX_SCRAM_ITERATIONS, MIN_SCRAM_ITERATIONS, PatternType,
    PermissionType, ResourceType, ScramDeletion, ScramIterations, ScramUpsertion, ScramUserOutcome,
    UserScramCredential, UserScramCredentials,
};

/// Test seam for `AdminClient`.
///
/// The operator's reconcile only needs dynamic dispatch through this trait.
/// Production code wraps a concrete `AdminClient`, and tests substitute a
/// fake.
///
/// Methods take `&mut self` because the underlying `AdminClient`'s
/// `NOT_CONTROLLER` retry path reconnects the inner `Connection` in place,
/// which needs unique access.
#[async_trait::async_trait]
pub trait AdminClientLike: Send {
    /// Finalize `metadata.version`, using Kafka's safe-downgrade mode when the
    /// target is below the previously finalized level.
    async fn update_metadata_version(
        &mut self,
        _level: i16,
        _safe_downgrade: bool,
        _timeout: Time,
    ) -> Result<MetadataVersionUpdate, AdminError> {
        Err(AdminError::Protocol(
            "UpdateFeatures is not implemented by this admin client".into(),
        ))
    }
    /// Read committed metadata-quorum membership.
    async fn describe_metadata_quorum(&mut self) -> Result<MetadataQuorum, AdminError> {
        Err(AdminError::Protocol(
            "DescribeQuorum is not implemented by this admin client".into(),
        ))
    }
    /// Remove one exact metadata-quorum voter identity.
    async fn remove_raft_voter(
        &mut self,
        _cluster_id: uuid::Uuid,
        _node_id: i32,
        _directory_id: uuid::Uuid,
    ) -> Result<(), AdminError> {
        Err(AdminError::Protocol(
            "RemoveRaftVoter is not implemented by this admin client".into(),
        ))
    }
    async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError>;
    async fn reconcile_topic_replication_factor(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError>;
    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout: Time,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError>;
    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout: Time,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError>;
    async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout: Time,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError>;
    async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        timeout: Time,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError>;
    async fn describe_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<TopicConfigOverrides>, AdminError>;
    async fn incremental_alter_configs(
        &mut self,
        ops: &[IncrementalAlterOp],
    ) -> Result<Vec<AlterConfigsOutcome>, AdminError>;
    async fn alter_user_scram_credentials_sha512(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError>;
    /// SCRAM-SHA-256 sibling of
    /// [`Self::alter_user_scram_credentials_sha512`]. The operator
    /// calls this when a `KafkaUser.spec.authentication.type ==
    /// scram-sha-256`.
    async fn alter_user_scram_credentials_sha256(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError>;
    async fn describe_acls(&mut self, filter: &AclEntryFilter)
    -> Result<Vec<AclEntry>, AdminError>;
    async fn create_acls(
        &mut self,
        creations: &[AclEntry],
    ) -> Result<Vec<CreateAclOutcome>, AdminError>;
    async fn delete_acls(
        &mut self,
        filters: &[AclEntryFilter],
    ) -> Result<Vec<DeleteAclFilterOutcome>, AdminError>;
    async fn describe_user_quotas(&mut self, username: &str)
    -> Result<UserQuotaConfig, AdminError>;
    async fn alter_user_quotas(
        &mut self,
        username: &str,
        ops: &[QuotaOp],
        validate_only: bool,
    ) -> Result<Option<KafkaError>, AdminError>;

    // ── delegation-token RPCs (KIP-48) ────────────────────────────────
    //
    // Trait-level return type is `crabka_metadata::DelegationToken`
    // (the image type) rather than the raw `Create/RenewDelegationToken`
    // response. The `AdminClientLike for AdminClient` impl below
    // reshapes wire responses into the image type — see the per-method
    // comments there for the trade-off on how the renew path recovers
    // the full token (the renew response carries only the new expiry).
    async fn create_delegation_token_as_owner(
        &mut self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime: Option<Time>,
    ) -> Result<crabka_metadata::DelegationToken, AdminError>;
    async fn renew_delegation_token(
        &mut self,
        hmac: &[u8],
    ) -> Result<crabka_metadata::DelegationToken, AdminError>;
    async fn expire_delegation_token(&mut self, hmac: &[u8]) -> Result<(), AdminError>;
    async fn describe_delegation_tokens_owned_by(
        &mut self,
        owner_principal: &str,
    ) -> Result<Vec<crabka_metadata::DelegationToken>, AdminError>;
}

#[async_trait::async_trait]
impl AdminClientLike for AdminClient {
    async fn update_metadata_version(
        &mut self,
        level: i16,
        safe_downgrade: bool,
        timeout: Time,
    ) -> Result<MetadataVersionUpdate, AdminError> {
        AdminClient::update_metadata_version(self, level, safe_downgrade, timeout).await
    }

    async fn describe_metadata_quorum(&mut self) -> Result<MetadataQuorum, AdminError> {
        AdminClient::describe_metadata_quorum(self).await
    }

    async fn remove_raft_voter(
        &mut self,
        cluster_id: uuid::Uuid,
        node_id: i32,
        directory_id: uuid::Uuid,
    ) -> Result<(), AdminError> {
        AdminClient::remove_raft_voter(self, cluster_id, node_id, directory_id).await
    }

    async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        AdminClient::metadata(self, topics).await
    }
    async fn reconcile_topic_replication_factor(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError> {
        AdminClient::reconcile_topic_replication_factor(self, topic, replication_factor, timeout)
            .await
    }
    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout: Time,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        AdminClient::create_topics(self, specs, timeout).await
    }
    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout: Time,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        AdminClient::delete_topics(self, names, timeout).await
    }
    async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout: Time,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        AdminClient::create_partitions(self, ops, timeout).await
    }
    async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        timeout: Time,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError> {
        AdminClient::delete_records(self, ops, timeout).await
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
    async fn alter_user_scram_credentials_sha512(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        AdminClient::alter_user_scram_credentials_sha512(self, upsertions, deletions).await
    }
    async fn alter_user_scram_credentials_sha256(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        AdminClient::alter_user_scram_credentials_sha256(self, upsertions, deletions).await
    }
    async fn describe_acls(
        &mut self,
        filter: &AclEntryFilter,
    ) -> Result<Vec<AclEntry>, AdminError> {
        AdminClient::describe_acls(self, filter).await
    }
    async fn create_acls(
        &mut self,
        creations: &[AclEntry],
    ) -> Result<Vec<CreateAclOutcome>, AdminError> {
        AdminClient::create_acls(self, creations).await
    }
    async fn delete_acls(
        &mut self,
        filters: &[AclEntryFilter],
    ) -> Result<Vec<DeleteAclFilterOutcome>, AdminError> {
        AdminClient::delete_acls(self, filters).await
    }
    async fn describe_user_quotas(
        &mut self,
        username: &str,
    ) -> Result<UserQuotaConfig, AdminError> {
        AdminClient::describe_user_quotas(self, username).await
    }
    async fn alter_user_quotas(
        &mut self,
        username: &str,
        ops: &[QuotaOp],
        validate_only: bool,
    ) -> Result<Option<KafkaError>, AdminError> {
        AdminClient::alter_user_quotas(self, username, ops, validate_only).await
    }

    // ── delegation-token RPCs ─────────────────────────────────────────
    //
    // The inherent `AdminClient` methods in `delegation_tokens.rs`
    // return the wire-shaped response (`CreateDelegationTokenResponse`
    // for create, `i64` new expiry for renew, `()` for expire, image
    // `DelegationToken` for describe). The trait surface is normalised
    // to `crabka_metadata::DelegationToken` so the operator's reconcile
    // path is wire-agnostic.
    async fn create_delegation_token_as_owner(
        &mut self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime: Option<Time>,
    ) -> Result<crabka_metadata::DelegationToken, AdminError> {
        // The create-response carries every field the image type needs
        // *except* the renewer list (the broker does not echo it back),
        // so we reconstruct that from the caller's input — which is the
        // ground truth anyway (KIP-48's create accepts the renewer set
        // verbatim and the broker stores it as-is).
        let resp = AdminClient::create_delegation_token_as_owner(
            self,
            owner_principal_name,
            renewers,
            max_lifetime,
        )
        .await?;
        let renewers_image = renewers
            .iter()
            .filter_map(|s| renewer_str_to_principal(s))
            .collect();
        Ok(crabka_metadata::DelegationToken {
            token_id: resp.token_id,
            owner: crabka_security::KafkaPrincipal {
                principal_type: resp.principal_type,
                name: resp.principal_name,
            },
            hmac: resp.hmac.to_vec(),
            issue_timestamp_ms: resp.issue_timestamp_ms,
            expiry_timestamp_ms: resp.expiry_timestamp_ms,
            max_timestamp_ms: resp.max_timestamp_ms,
            renewers: renewers_image,
        })
    }

    async fn renew_delegation_token(
        &mut self,
        hmac: &[u8],
    ) -> Result<crabka_metadata::DelegationToken, AdminError> {
        // The renew-response (`RenewDelegationTokenResponse`) carries
        // only the new `expiry_timestamp_ms`. To rebuild the full
        // `DelegationToken` we follow up with `DescribeDelegationToken`
        // with `owners=None` (describe all) and look up the entry by
        // hmac. This adds one RPC per renewal — acceptable because the
        // operator renews each user at most every `renew_before_expiry_ms`
        // (24h by default). An alternative would have been to thread the
        // owner principal into the trait method; keeping the surface
        // `hmac`-only matches the inherent O3 signature.
        let _new_expiry = AdminClient::renew_delegation_token(self, hmac).await?;
        let req = crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::default();
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::Broker {
                api: "DescribeDelegationToken",
                code: resp.error_code,
                name: kafka_error_name(resp.error_code),
                message: None,
            });
        }
        let matched = resp
            .tokens
            .into_iter()
            .find(|t| t.hmac.as_ref() == hmac)
            .ok_or_else(|| {
                AdminError::Protocol(
                    "RenewDelegationToken: follow-up describe did not return the renewed token"
                        .into(),
                )
            })?;
        Ok(crabka_metadata::DelegationToken {
            token_id: matched.token_id,
            owner: crabka_security::KafkaPrincipal {
                principal_type: matched.principal_type,
                name: matched.principal_name,
            },
            hmac: matched.hmac.to_vec(),
            issue_timestamp_ms: matched.issue_timestamp,
            expiry_timestamp_ms: matched.expiry_timestamp,
            max_timestamp_ms: matched.max_timestamp,
            renewers: matched
                .renewers
                .into_iter()
                .map(|r| crabka_security::KafkaPrincipal {
                    principal_type: r.principal_type,
                    name: r.principal_name,
                })
                .collect(),
        })
    }

    async fn expire_delegation_token(&mut self, hmac: &[u8]) -> Result<(), AdminError> {
        AdminClient::expire_delegation_token(self, hmac).await
    }

    async fn describe_delegation_tokens_owned_by(
        &mut self,
        owner_principal: &str,
    ) -> Result<Vec<crabka_metadata::DelegationToken>, AdminError> {
        AdminClient::describe_delegation_tokens_owned_by(self, owner_principal).await
    }
}

/// Splits `"Type:Name"` into a `KafkaPrincipal`. The default type is `User`.
/// Empty input gives `None`, so the create path does not manufacture a
/// principal from a bare `""` renewer entry.
fn renewer_str_to_principal(s: &str) -> Option<crabka_security::KafkaPrincipal> {
    if s.is_empty() {
        return None;
    }
    let (pt, pn) = s.split_once(':').unwrap_or(("User", s));
    Some(crabka_security::KafkaPrincipal {
        principal_type: pt.to_string(),
        name: pn.to_string(),
    })
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaError {
    pub code: i16,
    pub name: &'static str,
    pub message: Option<String>,
}

pub(crate) fn kafka_error_if(code: i16, message: Option<String>) -> Option<KafkaError> {
    if code == 0 {
        None
    } else {
        Some(KafkaError {
            code,
            name: kafka_error_name(code),
            message,
        })
    }
}

async fn lookup_first<F, I>(
    host_port: &str,
    dns_timeout: crabka_client_core::ClientDnsTimeout,
    lookup: F,
) -> Result<std::net::SocketAddr, AdminError>
where
    F: std::future::Future<Output = std::io::Result<I>>,
    I: Iterator<Item = std::net::SocketAddr>,
{
    let mut addrs = tokio::time::timeout(dns_timeout.time().to_std(), lookup)
        .await
        .map_err(|_| {
            AdminError::Protocol(format!(
                "DNS lookup {host_port} timed out after {} ms",
                dns_timeout.milliseconds(),
            ))
        })?
        .map_err(|error| AdminError::Protocol(format!("DNS lookup {host_port}: {error}")))?;
    addrs
        .next()
        .ok_or_else(|| AdminError::Protocol(format!("no addresses for {host_port}")))
}

fn format_host_port(host: &str, port: i32) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Short-lived admin client that targets one cluster's controller. It can
/// negotiate TLS/SASL through [`AdminClient::connect_secured`].
pub struct AdminClient {
    pub(crate) conn: RecoveringConnection,
    bootstrap_addrs: Vec<String>,
    /// Full connection template carried forward so reconnects preserve
    /// caller-supplied identity, security, and timeouts.
    options: ConnectionOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BootstrapTarget {
    Brokers,
    Controllers,
}

struct ConnectedTarget {
    connection: Connection,
    discovered_addrs: Vec<String>,
}

/// One controller connection with KIP-919/KIP-1102 bootstrap recovery.
pub(crate) struct RecoveringConnection {
    inner: tokio::sync::RwLock<Connection>,
    bootstrap_addrs: Vec<String>,
    options: ConnectionOptions,
    strategy: MetadataRecoveryStrategy,
    trigger: MetadataRecoveryRebootstrapTrigger,
    target: BootstrapTarget,
    known_addrs: Mutex<Vec<String>>,
    first_metadata_attempt: Mutex<Option<tokio::time::Instant>>,
}

impl RecoveringConnection {
    fn new(
        connection: Connection,
        bootstrap_addrs: Vec<String>,
        options: ConnectionOptions,
        strategy: MetadataRecoveryStrategy,
        trigger: MetadataRecoveryRebootstrapTrigger,
        target: BootstrapTarget,
        known_addrs: Vec<String>,
    ) -> Self {
        Self {
            inner: tokio::sync::RwLock::new(connection),
            bootstrap_addrs,
            options,
            strategy,
            trigger,
            target,
            known_addrs: Mutex::new(known_addrs),
            first_metadata_attempt: Mutex::new(None),
        }
    }

    pub(crate) async fn send<R>(&self, request: R) -> Result<R::Response, AdminError>
    where
        R: crabka_protocol::ProtocolRequest + Clone,
        R::Response: 'static,
    {
        self.send_with_min_version(request, None).await
    }

    pub(crate) async fn send_at_least<R>(
        &self,
        request: R,
        min_version: i16,
    ) -> Result<R::Response, AdminError>
    where
        R: crabka_protocol::ProtocolRequest + Clone,
        R::Response: 'static,
    {
        self.send_with_min_version(request, Some(min_version)).await
    }

    async fn send_with_min_version<R>(
        &self,
        request: R,
        min_version: Option<i16>,
    ) -> Result<R::Response, AdminError>
    where
        R: crabka_protocol::ProtocolRequest + Clone,
        R::Response: 'static,
    {
        self.require_advertised_controller_api::<R>().await?;
        self.begin_metadata_attempt::<R>();
        let first = self.send_current(request.clone(), min_version).await;
        match first {
            Ok(response) if Self::response_requires_rebootstrap(&response) => {
                if self.strategy == MetadataRecoveryStrategy::None {
                    return Err(AdminError::Transport(ClientError::Server {
                        error_code: 129,
                    }));
                }
                self.send_after_rebootstrap(request, min_version).await
            }
            Ok(response) if self.metadata_attempt_timed_out(&response) => {
                self.send_after_rebootstrap(request, min_version).await
            }
            Ok(response) => {
                self.observe_cluster_endpoints(&response);
                self.complete_metadata_attempt(&response);
                Ok(response)
            }
            Err(error) if AdminClient::is_retriable_transport_error(&error) => {
                match self
                    .send_after_current_metadata(request.clone(), min_version)
                    .await
                {
                    Ok(response) => Ok(response),
                    Err(_) if self.strategy == MetadataRecoveryStrategy::Rebootstrap => {
                        self.send_after_rebootstrap(request, min_version).await
                    }
                    Err(_) => Err(AdminError::from(error)),
                }
            }
            Err(error) => Err(AdminError::from(error)),
        }
    }

    async fn replace(&self, connection: Connection) {
        *self.inner.write().await = connection;
    }

    async fn send_current<R>(
        &self,
        request: R,
        min_version: Option<i16>,
    ) -> Result<R::Response, ClientError>
    where
        R: crabka_protocol::ProtocolRequest,
    {
        let connection = self.inner.read().await;
        if let Some(min_version) = min_version {
            return send_connection_at_least(&connection, request, min_version).await;
        }
        connection.send(request).await
    }

    async fn send_after_rebootstrap<R>(
        &self,
        request: R,
        min_version: Option<i16>,
    ) -> Result<R::Response, AdminError>
    where
        R: crabka_protocol::ProtocolRequest,
        R::Response: 'static,
    {
        self.rebootstrap().await?;
        self.require_advertised_controller_api::<R>().await?;
        self.begin_metadata_attempt::<R>();
        let response = self.send_current(request, min_version).await?;
        if Self::response_requires_rebootstrap(&response) {
            return Err(AdminError::Transport(ClientError::Server {
                error_code: 129,
            }));
        }
        self.observe_cluster_endpoints(&response);
        self.complete_metadata_attempt(&response);
        Ok(response)
    }

    async fn send_after_current_metadata<R>(
        &self,
        request: R,
        min_version: Option<i16>,
    ) -> Result<R::Response, AdminError>
    where
        R: crabka_protocol::ProtocolRequest + Clone,
        R::Response: 'static,
    {
        self.reconnect_current_metadata().await?;
        self.require_advertised_controller_api::<R>().await?;
        let response = self.send_current(request.clone(), min_version).await?;
        if Self::response_requires_rebootstrap(&response) {
            if self.strategy == MetadataRecoveryStrategy::Rebootstrap {
                return self.send_after_rebootstrap(request, min_version).await;
            }
            return Err(AdminError::Transport(ClientError::Server {
                error_code: 129,
            }));
        }
        if self.metadata_attempt_timed_out(&response) {
            return self.send_after_rebootstrap(request, min_version).await;
        }
        self.observe_cluster_endpoints(&response);
        self.complete_metadata_attempt(&response);
        Ok(response)
    }

    async fn require_advertised_controller_api<R>(&self) -> Result<(), AdminError>
    where
        R: crabka_protocol::ProtocolRequest,
    {
        const UNSUPPORTED_ENDPOINT_TYPE: i16 = 115;
        if self.target == BootstrapTarget::Controllers
            && self
                .inner
                .read()
                .await
                .advertised_api_range(R::API_KEY)
                .is_none()
        {
            return Err(AdminError::Broker {
                api: "ControllerEndpoint",
                code: UNSUPPORTED_ENDPOINT_TYPE,
                name: kafka_error_name(UNSUPPORTED_ENDPOINT_TYPE),
                message: Some(format!(
                    "api_key {} is not supported by the controller listener",
                    R::API_KEY
                )),
            });
        }
        Ok(())
    }

    pub(crate) async fn rebootstrap(&self) -> Result<(), AdminError> {
        for host_port in &self.bootstrap_addrs {
            if let Ok(connected) = AdminClient::connect_target_one_with_discovery(
                host_port,
                self.options.clone(),
                self.target,
            )
            .await
            {
                self.replace(connected.connection).await;
                self.replace_known_addrs(if connected.discovered_addrs.is_empty() {
                    self.bootstrap_addrs.clone()
                } else {
                    connected.discovered_addrs
                });
                self.clear_metadata_attempt();
                return Ok(());
            }
        }
        Err(AdminError::Connect {
            tried: self.bootstrap_addrs.len(),
        })
    }

    async fn reconnect_current_metadata(&self) -> Result<(), AdminError> {
        let known_addrs = self
            .known_addrs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for host_port in &known_addrs {
            if let Ok(connected) = AdminClient::connect_target_one_with_discovery(
                host_port,
                self.options.clone(),
                self.target,
            )
            .await
            {
                self.replace(connected.connection).await;
                if !connected.discovered_addrs.is_empty() {
                    self.replace_known_addrs(connected.discovered_addrs);
                }
                return Ok(());
            }
        }
        Err(AdminError::Connect {
            tried: known_addrs.len(),
        })
    }

    fn replace_known_addrs(&self, addrs: Vec<String>) {
        *self
            .known_addrs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = addrs;
    }

    fn observe_cluster_endpoints<T: Any>(&self, response: &T) {
        if let Some(metadata) = (response as &dyn Any)
            .downcast_ref::<crabka_protocol::owned::metadata_response::MetadataResponse>(
        ) {
            let endpoints = metadata
                .brokers
                .iter()
                .filter(|broker| !broker.host.is_empty() && broker.port > 0)
                .map(|broker| format_host_port(&broker.host, broker.port))
                .collect::<Vec<_>>();
            if !endpoints.is_empty() {
                self.replace_known_addrs(endpoints);
            }
            return;
        }
        if self.target == BootstrapTarget::Controllers
            && let Some(cluster) = (response as &dyn Any).downcast_ref::<
                crabka_protocol::owned::describe_cluster_response::DescribeClusterResponse,
            >()
        {
            let endpoints = cluster
                .brokers
                .iter()
                .filter(|controller| !controller.host.is_empty() && controller.port > 0)
                .map(|controller| format_host_port(&controller.host, controller.port))
                .collect::<Vec<_>>();
            if !endpoints.is_empty() {
                self.replace_known_addrs(endpoints);
            }
        }
    }

    pub(crate) fn uses_controller_bootstrap(&self) -> bool {
        self.target == BootstrapTarget::Controllers
    }

    fn begin_metadata_attempt<R: crabka_protocol::ProtocolRequest>(&self) {
        if R::API_KEY != crabka_protocol::owned::metadata_request::MetadataRequest::API_KEY
            || self.strategy == MetadataRecoveryStrategy::None
        {
            return;
        }
        self.first_metadata_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(tokio::time::Instant::now);
    }

    fn response_requires_rebootstrap<T: Any>(response: &T) -> bool {
        (response as &dyn Any)
            .downcast_ref::<crabka_protocol::owned::metadata_response::MetadataResponse>()
            .is_some_and(|metadata| metadata.error_code == 129)
    }

    fn metadata_attempt_timed_out<T: Any>(&self, response: &T) -> bool {
        if self.strategy == MetadataRecoveryStrategy::None {
            return false;
        }
        let Some(metadata) = (response as &dyn Any)
            .downcast_ref::<crabka_protocol::owned::metadata_response::MetadataResponse>(
        ) else {
            return false;
        };
        metadata.brokers.is_empty()
            && self
                .first_metadata_attempt
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some_and(|started| started.elapsed() >= self.trigger.time().to_std())
    }

    fn complete_metadata_attempt<T: Any>(&self, response: &T) {
        let has_brokers = (response as &dyn Any)
            .downcast_ref::<crabka_protocol::owned::metadata_response::MetadataResponse>()
            .is_some_and(|metadata| !metadata.brokers.is_empty());
        if has_brokers {
            self.clear_metadata_attempt();
        }
    }

    fn clear_metadata_attempt(&self) {
        *self
            .first_metadata_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

async fn send_connection_at_least<R>(
    connection: &Connection,
    request: R,
    min_version: i16,
) -> Result<R::Response, ClientError>
where
    R: crabka_protocol::ProtocolRequest,
{
    let (broker_min, broker_max) = connection
        .advertised_api_range(R::API_KEY)
        .unwrap_or((0, 0));
    let client_min = R::MIN_VERSION.max(min_version);
    let chosen = R::MAX_VERSION.min(broker_max);
    if chosen < client_min || chosen < broker_min {
        return Err(ClientError::IncompatibleVersion {
            api_key: R::API_KEY,
            broker_min,
            broker_max,
            client_min,
            client_max: R::MAX_VERSION,
        });
    }
    connection.send(request).await
}

impl AdminClient {
    /// Builds the per-connect options for `client_id="crabka-operator"` with
    /// the supplied security policy.
    fn opts(security: Option<crabka_client_core::security::ClientSecurity>) -> ConnectionOptions {
        ConnectionOptions {
            dns_timeout: crabka_client_core::ClientDnsTimeout::default(),
            connect_timeout: secs(5),
            request_timeout: secs(30),
            client_id: "crabka-operator".to_string(),
            dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity::default(),
            frame_max: crabka_client_core::ClientFrameMax::default(),
            security: security.map(Box::new),
        }
    }

    /// Connects and applies optional client security. `None` means plaintext,
    /// which is identical to [`AdminClient::connect`].
    ///
    /// # Errors
    /// Returns `AdminError::Connect { tried }` if no bootstrap address
    /// accepted the (optionally secured) connection.
    pub async fn connect_secured(
        bootstrap_addrs: &[String],
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, AdminError> {
        Self::connect_with_options(bootstrap_addrs, Self::opts(security)).await
    }

    /// Connects with the supplied security and DNS deadline, and keeps the
    /// standard admin identity, TCP-connect timeout, and request timeout.
    ///
    /// # Errors
    /// Returns `AdminError::Connect { tried }` if no bootstrap address connects.
    pub async fn connect_secured_with_dns_timeout(
        bootstrap_addrs: &[String],
        security: Option<crabka_client_core::security::ClientSecurity>,
        dns_timeout: crabka_client_core::ClientDnsTimeout,
    ) -> Result<Self, AdminError> {
        let mut options = Self::opts(security);
        options.dns_timeout = dns_timeout;
        Self::connect_with_options(bootstrap_addrs, options).await
    }

    /// Connects with the standard plaintext admin policy and a custom DNS
    /// deadline.
    ///
    /// # Errors
    /// Returns `AdminError::Connect { tried }` if no bootstrap address connects.
    pub async fn connect_with_dns_timeout(
        bootstrap_addrs: &[String],
        dns_timeout: crabka_client_core::ClientDnsTimeout,
    ) -> Result<Self, AdminError> {
        Self::connect_secured_with_dns_timeout(bootstrap_addrs, None, dns_timeout).await
    }

    /// Connects with a complete connection-options template.
    ///
    /// # Errors
    /// Returns `AdminError::Connect { tried }` if no bootstrap address
    /// accepted the connection.
    pub async fn connect_with_options(
        bootstrap_addrs: &[String],
        options: ConnectionOptions,
    ) -> Result<Self, AdminError> {
        Self::connect_with_metadata_recovery_target(
            bootstrap_addrs,
            options,
            MetadataRecoveryStrategy::default(),
            crabka_client_core::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER,
            BootstrapTarget::Brokers,
        )
        .await
    }

    /// Connects through KIP-919 controller bootstrap addresses rather than
    /// broker bootstrap addresses.
    ///
    /// # Errors
    /// Returns a connection, protocol, or broker error when no controller
    /// bootstrap endpoint can identify and connect to the active controller.
    pub async fn connect_controller(bootstrap_controllers: &[String]) -> Result<Self, AdminError> {
        Self::connect_controller_secured(bootstrap_controllers, None).await
    }

    /// Controller-bootstrap variant of [`Self::connect_secured`].
    ///
    /// # Errors
    /// Returns a connection, protocol, or broker error when no controller
    /// bootstrap endpoint can identify and connect to the active controller.
    pub async fn connect_controller_secured(
        bootstrap_controllers: &[String],
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, AdminError> {
        Self::connect_controller_with_options(bootstrap_controllers, Self::opts(security)).await
    }

    /// Controller-bootstrap variant of [`Self::connect_with_options`].
    ///
    /// # Errors
    /// Returns a connection, protocol, or broker error when no controller
    /// bootstrap endpoint can identify and connect to the active controller.
    pub async fn connect_controller_with_options(
        bootstrap_controllers: &[String],
        options: ConnectionOptions,
    ) -> Result<Self, AdminError> {
        Self::connect_with_metadata_recovery_target(
            bootstrap_controllers,
            options,
            MetadataRecoveryStrategy::default(),
            crabka_client_core::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER,
            BootstrapTarget::Controllers,
        )
        .await
    }

    /// Connect with explicit KIP-1102 metadata recovery settings.
    ///
    /// # Errors
    /// Returns an invalid-configuration error for a negative or fractional
    /// millisecond trigger, or a connection error when no bootstrap endpoint
    /// is reachable.
    pub async fn connect_with_metadata_recovery(
        bootstrap_addrs: &[String],
        options: ConnectionOptions,
        strategy: MetadataRecoveryStrategy,
        rebootstrap_trigger: Time,
    ) -> Result<Self, AdminError> {
        Self::connect_with_metadata_recovery_target(
            bootstrap_addrs,
            options,
            strategy,
            rebootstrap_trigger,
            BootstrapTarget::Brokers,
        )
        .await
    }

    async fn connect_with_metadata_recovery_target(
        bootstrap_addrs: &[String],
        options: ConnectionOptions,
        strategy: MetadataRecoveryStrategy,
        rebootstrap_trigger: Time,
        target: BootstrapTarget,
    ) -> Result<Self, AdminError> {
        let trigger = MetadataRecoveryRebootstrapTrigger::new(rebootstrap_trigger)
            .map_err(AdminError::Protocol)?;
        let mut last_error = None;
        for host_port in bootstrap_addrs {
            match Self::connect_target_one_with_discovery(host_port, options.clone(), target).await
            {
                Ok(connected) => {
                    let known_addrs = if connected.discovered_addrs.is_empty() {
                        bootstrap_addrs.to_vec()
                    } else {
                        connected.discovered_addrs
                    };
                    return Ok(Self {
                        conn: RecoveringConnection::new(
                            connected.connection,
                            bootstrap_addrs.to_vec(),
                            options.clone(),
                            strategy,
                            trigger,
                            target,
                            known_addrs,
                        ),
                        bootstrap_addrs: bootstrap_addrs.to_vec(),
                        options,
                    });
                }
                Err(e) => {
                    tracing::debug!(
                        target: "crabka_client_admin",
                        addr = %host_port,
                        error = %e,
                        "bootstrap connect failed",
                    );
                    last_error = Some(e);
                }
            }
        }
        if target == BootstrapTarget::Controllers
            && let Some(error) = last_error
        {
            return Err(error);
        }
        Err(AdminError::Connect {
            tried: bootstrap_addrs.len(),
        })
    }

    /// Tries each bootstrap address in order.
    ///
    /// Each entry is `host:port`. `tokio::net::lookup_host` resolves the DNS.
    /// The first successful connect wins. Returns
    /// `AdminError::Connect { tried }` if none responded. The connection is
    /// plaintext. See [`AdminClient::connect_secured`].
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn connect(bootstrap_addrs: &[String]) -> Result<Self, AdminError> {
        Self::connect_secured(bootstrap_addrs, None).await
    }

    async fn connect_one(
        host_port: &str,
        opts: ConnectionOptions,
    ) -> Result<Connection, AdminError> {
        let addr = lookup_first(
            host_port,
            opts.dns_timeout,
            tokio::net::lookup_host(host_port),
        )
        .await?;
        Connection::connect_with_options(addr, opts)
            .await
            .map_err(AdminError::from)
    }

    #[cfg(test)]
    async fn connect_target_one(
        host_port: &str,
        options: ConnectionOptions,
        target: BootstrapTarget,
    ) -> Result<Connection, AdminError> {
        Self::connect_target_one_with_discovery(host_port, options, target)
            .await
            .map(|connected| connected.connection)
    }

    async fn connect_target_one_with_discovery(
        host_port: &str,
        options: ConnectionOptions,
        target: BootstrapTarget,
    ) -> Result<ConnectedTarget, AdminError> {
        match target {
            BootstrapTarget::Brokers => Ok(ConnectedTarget {
                connection: Self::connect_one(host_port, options).await?,
                discovered_addrs: Vec::new(),
            }),
            BootstrapTarget::Controllers => {
                Self::connect_controller_one_with_discovery(host_port, options).await
            }
        }
    }

    async fn connect_controller_one_with_discovery(
        host_port: &str,
        options: ConnectionOptions,
    ) -> Result<ConnectedTarget, AdminError> {
        use crabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;

        const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
        let bootstrap = Self::connect_one(host_port, options.clone()).await?;
        let response = send_connection_at_least(
            &bootstrap,
            DescribeClusterRequest {
                endpoint_type: ENDPOINT_TYPE_CONTROLLERS,
                ..Default::default()
            },
            1,
        )
        .await?;
        if response.error_code != 0 {
            return Err(AdminError::Broker {
                api: "DescribeCluster",
                code: response.error_code,
                name: kafka_error_name(response.error_code),
                message: response.error_message,
            });
        }
        if response.endpoint_type != ENDPOINT_TYPE_CONTROLLERS {
            return Err(AdminError::Protocol(format!(
                "DescribeCluster returned endpoint_type={}, expected CONTROLLERS",
                response.endpoint_type
            )));
        }
        let discovered_addrs = response
            .brokers
            .iter()
            .filter(|controller| !controller.host.is_empty() && controller.port > 0)
            .map(|controller| format_host_port(&controller.host, controller.port))
            .collect::<Vec<_>>();
        let controller = response
            .brokers
            .into_iter()
            .find(|controller| controller.broker_id == response.controller_id)
            .ok_or_else(|| {
                AdminError::Protocol(format!(
                    "DescribeCluster omitted controller_id={} from controller endpoints",
                    response.controller_id
                ))
            })?;
        if controller.host.is_empty() || controller.port <= 0 {
            return Err(AdminError::Protocol(format!(
                "DescribeCluster returned invalid controller endpoint {}:{}",
                controller.host, controller.port
            )));
        }
        let endpoint = format_host_port(&controller.host, controller.port);
        Ok(ConnectedTarget {
            connection: Self::connect_one(&endpoint, options).await?,
            discovered_addrs,
        })
    }

    /// Replaces the underlying connection. The `NOT_CONTROLLER` retry path
    /// uses it internally to reconnect to the current controller.
    pub(crate) async fn reconnect(&mut self, host_port: &str) -> Result<(), AdminError> {
        let opts = self.options.clone();
        self.conn
            .replace(Self::connect_one(host_port, opts).await?)
            .await;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn reconnect_bootstrap(&mut self) -> Result<(), AdminError> {
        let opts = self.options.clone();
        for host_port in &self.bootstrap_addrs {
            match Self::connect_target_one(host_port, opts.clone(), self.conn.target).await {
                Ok(conn) => {
                    self.conn.replace(conn).await;
                    return Ok(());
                }
                Err(error) => {
                    tracing::debug!(
                        target: "crabka_client_admin",
                        addr = %host_port,
                        error = %error,
                        "bootstrap reconnect failed",
                    );
                }
            }
        }
        Err(AdminError::Connect {
            tried: self.bootstrap_addrs.len(),
        })
    }

    pub(crate) fn is_retriable_transport_error(error: &ClientError) -> bool {
        matches!(
            error,
            ClientError::Timeout(_) | ClientError::Disconnected | ClientError::Io(_)
        )
    }
}

/// Kafka error code: the broker is not the controller (KIP-129). The
/// admin client refreshes its controller endpoint and retries once.
pub(crate) const NOT_CONTROLLER: i16 = 41;

/// Maps a Kafka error code into a static name string for human-friendly
/// `AdminError::Broker` output. The table holds only the codes this crate
/// surfaces today. Unknown codes serialize as `"UNKNOWN"`.
pub(crate) fn kafka_error_name(code: i16) -> &'static str {
    match code {
        0 => "NONE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        7 => "REQUEST_TIMED_OUT",
        14 => "COORDINATOR_LOAD_IN_PROGRESS",
        15 => "COORDINATOR_NOT_AVAILABLE",
        16 => "NOT_COORDINATOR",
        17 => "INVALID_TOPIC_EXCEPTION",
        19 => "NOT_ENOUGH_REPLICAS",
        31 => "CLUSTER_AUTHORIZATION_FAILED",
        33 => "UNSUPPORTED_SASL_MECHANISM",
        35 => "UNSUPPORTED_VERSION",
        36 => "TOPIC_ALREADY_EXISTS",
        37 => "INVALID_PARTITIONS",
        38 => "INVALID_REPLICATION_FACTOR",
        39 => "INVALID_REPLICA_ASSIGNMENT",
        40 => "INVALID_CONFIG",
        41 => "NOT_CONTROLLER",
        42 => "INVALID_REQUEST",
        47 => "INVALID_PRODUCER_EPOCH",
        48 => "INVALID_TXN_STATE",
        49 => "INVALID_PRODUCER_ID_MAPPING",
        51 => "CONCURRENT_TRANSACTIONS",
        53 => "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        66 => "DELEGATION_TOKEN_EXPIRED",
        83 => "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        84 => "ELECTION_NOT_NEEDED",
        91 => "RESOURCE_NOT_FOUND",
        92 => "DUPLICATE_RESOURCE",
        93 => "UNACCEPTABLE_CREDENTIAL",
        95 => "INVALID_UPDATE_VERSION",
        107 => "INELIGIBLE_REPLICA",
        114 => "MISMATCHED_ENDPOINT_TYPE",
        115 => "UNSUPPORTED_ENDPOINT_TYPE",
        116 => "UNKNOWN_CONTROLLER_ID",
        87 => "REASSIGNMENT_IN_PROGRESS",
        90 => "PRODUCER_FENCED",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicU16, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::{BufMut, BytesMut};
    use crabka_client_core::security::{ClientSecurity, SaslCredentials};
    use crabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_cluster_request::{self, DescribeClusterRequest},
            describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
            metadata_request,
            metadata_response::{
                FLEXIBLE_MIN as METADATA_FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
            },
            sasl_authenticate_request,
            sasl_authenticate_response::SaslAuthenticateResponse,
            sasl_handshake_request,
            sasl_handshake_response::SaslHandshakeResponse,
        },
    };
    use crabka_security::ListenerProtocol;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct ObservedAdminBroker {
        addr: std::net::SocketAddr,
        shutdown: CancellationToken,
        connections: Arc<AtomicUsize>,
        sasl_handshakes: Arc<AtomicUsize>,
        client_ids: Arc<Mutex<Vec<String>>>,
    }

    impl ObservedAdminBroker {
        async fn start(api_versions_delay: Duration) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let shutdown = CancellationToken::new();
            let connections = Arc::new(AtomicUsize::new(0));
            let sasl_handshakes = Arc::new(AtomicUsize::new(0));
            let client_ids = Arc::new(Mutex::new(Vec::new()));
            let task_shutdown = shutdown.clone();
            let task_connections = Arc::clone(&connections);
            let task_sasl_handshakes = Arc::clone(&sasl_handshakes);
            let task_client_ids = Arc::clone(&client_ids);

            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        break;
                    };
                    if task_shutdown.is_cancelled() {
                        break;
                    }
                    task_connections.fetch_add(1, Ordering::SeqCst);
                    let conn_sasl_handshakes = Arc::clone(&task_sasl_handshakes);
                    let conn_client_ids = Arc::clone(&task_client_ids);
                    tokio::spawn(async move {
                        while let Ok(frame_len) = stream.read_u32().await {
                            let mut request = vec![0_u8; frame_len as usize];
                            if stream.read_exact(&mut request).await.is_err() || request.len() < 10
                            {
                                break;
                            }
                            let api_key = i16::from_be_bytes([request[0], request[1]]);
                            let correlation_id =
                                i32::from_be_bytes(request[4..8].try_into().unwrap());
                            let client_id_len =
                                usize::from(u16::from_be_bytes([request[8], request[9]]));
                            if request.len() < 10 + client_id_len {
                                break;
                            }
                            conn_client_ids.lock().unwrap().push(
                                String::from_utf8_lossy(&request[10..10 + client_id_len]).into(),
                            );

                            let (body, flexible_header) = match api_key {
                                sasl_handshake_request::API_KEY => {
                                    conn_sasl_handshakes.fetch_add(1, Ordering::SeqCst);
                                    let mut body = BytesMut::new();
                                    SaslHandshakeResponse {
                                        error_code: 0,
                                        ..Default::default()
                                    }
                                    .encode(&mut body, 1)
                                    .unwrap();
                                    (body, false)
                                }
                                sasl_authenticate_request::API_KEY => {
                                    let mut body = BytesMut::new();
                                    SaslAuthenticateResponse {
                                        error_code: 0,
                                        ..Default::default()
                                    }
                                    .encode(&mut body, 2)
                                    .unwrap();
                                    (body, true)
                                }
                                api_versions_request::API_KEY => {
                                    tokio::time::sleep(api_versions_delay).await;
                                    let mut body = BytesMut::new();
                                    ApiVersionsResponse {
                                        error_code: 0,
                                        api_keys: vec![
                                            ApiVersion {
                                                api_key: api_versions_request::API_KEY,
                                                min_version: 0,
                                                max_version: 3,
                                                ..Default::default()
                                            },
                                            ApiVersion {
                                                api_key: metadata_request::API_KEY,
                                                min_version: 0,
                                                max_version: 12,
                                                ..Default::default()
                                            },
                                        ],
                                        ..Default::default()
                                    }
                                    .encode(&mut body, 0)
                                    .unwrap();
                                    (body, false)
                                }
                                _ => continue,
                            };
                            let mut response = BytesMut::new();
                            response.put_i32(correlation_id);
                            if flexible_header {
                                response.put_u8(0);
                            }
                            response.extend_from_slice(&body);
                            if stream
                                .write_u32(u32::try_from(response.len()).unwrap())
                                .await
                                .is_err()
                                || stream.write_all(&response).await.is_err()
                            {
                                break;
                            }
                        }
                    });
                }
            });

            Self {
                addr,
                shutdown,
                connections,
                sasl_handshakes,
                client_ids,
            }
        }

        fn observed_custom_security_and_id(&self) -> bool {
            self.sasl_handshakes.load(Ordering::SeqCst) > 0
                && self
                    .client_ids
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|client_id| client_id == "custom-admin")
        }

        fn stop(self) {
            self.shutdown.cancel();
        }
    }

    struct ObservedController {
        addr: std::net::SocketAddr,
        connections: Arc<AtomicUsize>,
        endpoint_types: Arc<Mutex<Vec<i8>>>,
    }

    impl ObservedController {
        async fn start(describe_cluster_max_version: i16) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let connections = Arc::new(AtomicUsize::new(0));
            let endpoint_types = Arc::new(Mutex::new(Vec::new()));
            let task_connections = Arc::clone(&connections);
            let task_endpoint_types = Arc::clone(&endpoint_types);

            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    task_connections.fetch_add(1, Ordering::SeqCst);
                    let endpoint_types = Arc::clone(&task_endpoint_types);
                    tokio::spawn(async move {
                        while let Ok(frame_len) = stream.read_u32().await {
                            let mut request = vec![0_u8; frame_len as usize];
                            if stream.read_exact(&mut request).await.is_err() || request.len() < 10
                            {
                                break;
                            }
                            let api_key = i16::from_be_bytes([request[0], request[1]]);
                            let api_version = i16::from_be_bytes([request[2], request[3]]);
                            let correlation_id =
                                i32::from_be_bytes(request[4..8].try_into().unwrap());
                            let client_id_len =
                                usize::from(u16::from_be_bytes([request[8], request[9]]));
                            let header_len = 10 + client_id_len;
                            if request.len() < header_len {
                                break;
                            }

                            let (body, flexible_header) = match api_key {
                                api_versions_request::API_KEY => {
                                    let mut body = BytesMut::new();
                                    ApiVersionsResponse {
                                        api_keys: vec![
                                            ApiVersion {
                                                api_key: api_versions_request::API_KEY,
                                                min_version: 0,
                                                max_version: 4,
                                                ..Default::default()
                                            },
                                            ApiVersion {
                                                api_key: describe_cluster_request::API_KEY,
                                                min_version: 0,
                                                max_version: describe_cluster_max_version,
                                                ..Default::default()
                                            },
                                        ],
                                        ..Default::default()
                                    }
                                    .encode(&mut body, 0)
                                    .unwrap();
                                    (body, false)
                                }
                                describe_cluster_request::API_KEY => {
                                    let mut encoded = &request[header_len + 1..];
                                    let decoded =
                                        DescribeClusterRequest::decode(&mut encoded, api_version)
                                            .unwrap();
                                    endpoint_types.lock().unwrap().push(decoded.endpoint_type);
                                    let mut body = BytesMut::new();
                                    DescribeClusterResponse {
                                        endpoint_type: 2,
                                        cluster_id: "controller-test".into(),
                                        controller_id: 1,
                                        brokers: vec![DescribeClusterBroker {
                                            broker_id: 1,
                                            host: addr.ip().to_string(),
                                            port: i32::from(addr.port()),
                                            ..Default::default()
                                        }],
                                        ..Default::default()
                                    }
                                    .encode(&mut body, api_version)
                                    .unwrap();
                                    (body, true)
                                }
                                _ => continue,
                            };
                            let mut response = BytesMut::new();
                            response.put_i32(correlation_id);
                            if flexible_header {
                                response.put_u8(0);
                            }
                            response.extend_from_slice(&body);
                            if stream
                                .write_u32(u32::try_from(response.len()).unwrap())
                                .await
                                .is_err()
                                || stream.write_all(&response).await.is_err()
                            {
                                break;
                            }
                        }
                    });
                }
            });

            Self {
                addr,
                connections,
                endpoint_types,
            }
        }
    }

    fn custom_admin_options() -> ConnectionOptions {
        ConnectionOptions {
            dns_timeout: crabka_client_core::ClientDnsTimeout::default(),
            client_id: "custom-admin".into(),
            connect_timeout: crabka_units::millis(100),
            request_timeout: crabka_units::millis(25),
            dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity::new(7)
                .unwrap(),
            frame_max: crabka_client_core::ClientFrameMax::try_from(crabka_units::kibibytes(32))
                .unwrap(),
            security: Some(Box::new(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Plain {
                    username: "u".into(),
                    password: "p".into(),
                }),
                sasl_host: Some("broker.example".into()),
            })),
        }
    }

    async fn metadata_times_out_with_custom_request_timeout(admin: &mut AdminClient) {
        let result = tokio::time::timeout(Duration::from_secs(2), admin.metadata(&[]))
            .await
            .expect("custom request timeout fires");
        assert2::assert!(result.is_err());
    }

    fn assert_custom_connect_timeout_is_stored(admin: &AdminClient) {
        assert2::assert!(admin.options.connect_timeout == crabka_units::millis(100));
        assert2::assert!(admin.options.dispatch_queue_capacity.get() == 7);
        assert2::assert!(admin.options.frame_max.size() == crabka_units::kibibytes(32));
    }

    fn metadata_mock_response(version: i16, broker_id: i32, port: u16) -> Vec<u8> {
        let response = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: broker_id,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut bytes = BytesMut::new();
        if version >= METADATA_FLEXIBLE_MIN {
            bytes.put_u8(0);
        }
        response.encode(&mut bytes, version).unwrap();
        bytes.to_vec()
    }

    fn admin_mock_api_versions() -> Vec<u8> {
        let response = ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: metadata_request::API_KEY,
                    min_version: 0,
                    max_version: 13,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut bytes = BytesMut::new();
        response.encode(&mut bytes, 0).unwrap();
        bytes.to_vec()
    }

    fn admin_mock_api_versions_with_describe_cluster(max_version: i16) -> Vec<u8> {
        let response = ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: describe_cluster_request::API_KEY,
                    min_version: 0,
                    max_version,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut bytes = BytesMut::new();
        response.encode(&mut bytes, 0).unwrap();
        bytes.to_vec()
    }

    async fn wait_for_admin_mock_closed(addr: std::net::SocketAddr) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while tokio::net::TcpStream::connect(addr).await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("mock broker listener closes");
    }

    #[test]
    fn kafka_error_name_known_codes() {
        for (_name, code, want) in [
            ("success", 0, "NONE"),
            ("topic exists", 36, "TOPIC_ALREADY_EXISTS"),
            ("not controller", 41, "NOT_CONTROLLER"),
        ] {
            assert2::assert!(kafka_error_name(code) == want);
        }
    }

    #[test]
    fn users_kafka_error_name_includes_scram_describe_codes() {
        for (_name, code, want) in [
            ("cluster authorization", 31, "CLUSTER_AUTHORIZATION_FAILED"),
            ("unsupported SASL", 33, "UNSUPPORTED_SASL_MECHANISM"),
            ("unsupported version", 35, "UNSUPPORTED_VERSION"),
            ("delegation token expired", 66, "DELEGATION_TOKEN_EXPIRED"),
            (
                "eligible leaders unavailable",
                83,
                "ELIGIBLE_LEADERS_NOT_AVAILABLE",
            ),
            ("election unnecessary", 84, "ELECTION_NOT_NEEDED"),
            ("resource not found", 91, "RESOURCE_NOT_FOUND"),
            ("duplicate resource", 92, "DUPLICATE_RESOURCE"),
            ("unacceptable credential", 93, "UNACCEPTABLE_CREDENTIAL"),
        ] {
            assert2::assert!(kafka_error_name(code) == want);
        }
    }

    #[test]
    fn users_kafka_error_name_includes_ineligible_replica() {
        assert2::assert!(kafka_error_name(107) == "INELIGIBLE_REPLICA");
    }

    #[test]
    fn kafka_error_name_unknown_returns_unknown() {
        assert2::assert!(kafka_error_name(9999) == "UNKNOWN");
    }

    #[test]
    fn kafka_error_if_zero_code_is_none() {
        assert2::assert!(kafka_error_if(0, None).is_none());
    }

    #[test]
    fn kafka_error_if_nonzero_carries_name() {
        let e = kafka_error_if(36, Some("dup".into())).unwrap();
        assert2::assert!(
            e == KafkaError {
                code: 36,
                name: "TOPIC_ALREADY_EXISTS",
                message: Some("dup".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn connect_secured_threads_security_and_fails_to_closed_port() {
        use crabka_client_core::security::{ClientSecurity, SaslCredentials};
        use crabka_security::ListenerProtocol;

        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        // 127.0.0.1:1 has no listener; the secured connect must fail —
        // proving the security arg is threaded (not a type error).
        let res = AdminClient::connect_secured(&["127.0.0.1:1".to_string()], Some(security)).await;
        assert2::assert!(res.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn dns_lookup_stops_at_connection_option_deadline() {
        let timeout = crabka_client_core::ClientDnsTimeout::new(Time::from_millis(37))
            .expect("positive timeout");
        let started = tokio::time::Instant::now();
        let pending =
            std::future::pending::<std::io::Result<std::vec::IntoIter<std::net::SocketAddr>>>();

        let result = lookup_first("broker.invalid:9092", timeout, pending).await;

        assert2::assert!(result.is_err());
        assert2::assert!(started.elapsed() == Duration::from_millis(37));
    }

    #[tokio::test]
    async fn custom_options_are_observable_on_initial_dial() {
        let live = ObservedAdminBroker::start(Duration::ZERO).await;
        let mut admin =
            AdminClient::connect_with_options(&[live.addr.to_string()], custom_admin_options())
                .await
                .unwrap();

        assert2::assert!(live.observed_custom_security_and_id());
        assert_custom_connect_timeout_is_stored(&admin);
        metadata_times_out_with_custom_request_timeout(&mut admin).await;

        let slow = ObservedAdminBroker::start(Duration::from_millis(300)).await;
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            AdminClient::connect_with_options(&[slow.addr.to_string()], custom_admin_options()),
        )
        .await
        .expect("ApiVersions obeys the stored connect timeout");
        assert2::assert!(result.is_err());

        live.stop();
        slow.stop();
    }

    #[tokio::test]
    async fn connect_with_dns_timeout_preserves_admin_defaults() {
        let live = ObservedAdminBroker::start(Duration::ZERO).await;
        let timeout = crabka_client_core::ClientDnsTimeout::new(Time::from_millis(37))
            .expect("positive timeout");
        let admin = AdminClient::connect_with_dns_timeout(&[live.addr.to_string()], timeout)
            .await
            .expect("admin connects");

        assert2::assert!(admin.options.dns_timeout == timeout);
        assert2::assert!(admin.options.client_id == "crabka-operator");
        assert2::assert!(admin.options.connect_timeout == secs(5));
        assert2::assert!(admin.options.request_timeout == secs(30));
        live.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_metadata_uses_a_learned_broker_after_the_only_seed_retires() {
        let healthy_port = Arc::new(AtomicU16::new(0));
        let healthy_calls = Arc::new(AtomicUsize::new(0));
        let h_port = healthy_port.clone();
        let h_calls = healthy_calls.clone();
        let healthy = crabka_client_core::MockBroker::start(
            move |api_key, version, _correlation_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(admin_mock_api_versions());
                }
                if api_key == metadata_request::API_KEY {
                    h_calls.fetch_add(1, Ordering::SeqCst);
                    return Some(metadata_mock_response(
                        version,
                        7,
                        h_port.load(Ordering::SeqCst),
                    ));
                }
                None
            },
        )
        .await;
        healthy_port.store(healthy.addr.port(), Ordering::SeqCst);

        let advertised_port = healthy_port.clone();
        let seed = crabka_client_core::MockBroker::start(
            move |api_key, version, _correlation_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(admin_mock_api_versions());
                }
                (api_key == metadata_request::API_KEY).then(|| {
                    metadata_mock_response(version, 7, advertised_port.load(Ordering::SeqCst))
                })
            },
        )
        .await;
        let admin = AdminClient::connect(&[seed.addr.to_string()])
            .await
            .expect("admin connects to its seed");
        let first = admin
            .conn
            .send(crabka_protocol::owned::metadata_request::MetadataRequest::default())
            .await
            .expect("seed provides last-known broker metadata");
        assert2::assert!(first.brokers[0].node_id == 7);

        let seed_addr = seed.addr;
        seed.stop();
        wait_for_admin_mock_closed(seed_addr).await;

        let refreshed = admin
            .conn
            .send(crabka_protocol::owned::metadata_request::MetadataRequest::default())
            .await
            .expect("admin refreshes through the learned broker");
        assert2::assert!(refreshed.brokers[0].node_id == 7);
        assert2::assert!(healthy_calls.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn recovering_connection_enforces_required_api_version() {
        let broker =
            crabka_client_core::MockBroker::start(|api_key, _version, _correlation_id, _body| {
                (api_key == api_versions_request::API_KEY)
                    .then(|| admin_mock_api_versions_with_describe_cluster(1))
            })
            .await;
        let admin = AdminClient::connect(&[broker.addr.to_string()])
            .await
            .expect("admin connects");

        let result = admin
            .conn
            .send_at_least(DescribeClusterRequest::default(), 2)
            .await;

        assert2::assert!(matches!(
            result,
            Err(AdminError::Transport(ClientError::IncompatibleVersion {
                api_key: describe_cluster_request::API_KEY,
                broker_max: 1,
                client_min: 2,
                ..
            }))
        ));
        broker.stop();
    }

    #[tokio::test]
    async fn controller_bootstrap_discovers_and_connects_active_controller() {
        let controller = ObservedController::start(2).await;

        let admin = AdminClient::connect_controller(&[controller.addr.to_string()])
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "controller bootstrap: {error}; connections={}; endpoint_types={:?}",
                    controller.connections.load(Ordering::SeqCst),
                    controller.endpoint_types.lock().unwrap(),
                )
            });

        assert2::assert!(admin.conn.uses_controller_bootstrap());
        assert2::assert!(controller.connections.load(Ordering::SeqCst) == 2);
        assert2::assert!(*controller.endpoint_types.lock().unwrap() == vec![2]);
    }

    #[tokio::test]
    async fn controller_bootstrap_requires_endpoint_type_version() {
        let controller = ObservedController::start(0).await;

        let result = AdminClient::connect_controller(&[controller.addr.to_string()]).await;

        assert2::assert!(matches!(
            result,
            Err(AdminError::Transport(ClientError::IncompatibleVersion {
                api_key: describe_cluster_request::API_KEY,
                broker_max: 0,
                client_min: 1,
                ..
            }))
        ));
        assert2::assert!(controller.endpoint_types.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn secured_dns_timeout_preserves_security_and_admin_defaults() {
        let live = ObservedAdminBroker::start(Duration::ZERO).await;
        let timeout = crabka_client_core::ClientDnsTimeout::new(Time::from_millis(37))
            .expect("positive timeout");
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: Some("broker.example".into()),
        };
        let admin = AdminClient::connect_secured_with_dns_timeout(
            &[live.addr.to_string()],
            Some(security),
            timeout,
        )
        .await
        .expect("secured admin connects");

        assert2::assert!(admin.options.dns_timeout == timeout);
        assert2::assert!(admin.options.security.is_some());
        assert2::assert!(admin.options.client_id == "crabka-operator");
        assert2::assert!(admin.options.connect_timeout == secs(5));
        assert2::assert!(admin.options.request_timeout == secs(30));
        live.stop();
    }

    #[tokio::test]
    async fn custom_options_are_observable_on_controller_reconnect() {
        let bootstrap = ObservedAdminBroker::start(Duration::ZERO).await;
        let controller = ObservedAdminBroker::start(Duration::ZERO).await;
        let slow = ObservedAdminBroker::start(Duration::from_millis(300)).await;
        let mut admin = AdminClient::connect_with_options(
            &[bootstrap.addr.to_string()],
            custom_admin_options(),
        )
        .await
        .unwrap();

        admin.reconnect(&controller.addr.to_string()).await.unwrap();
        assert2::assert!(controller.observed_custom_security_and_id());
        assert_custom_connect_timeout_is_stored(&admin);
        metadata_times_out_with_custom_request_timeout(&mut admin).await;
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            admin.reconnect(&slow.addr.to_string()),
        )
        .await
        .expect("reconnected ApiVersions obeys the stored connect timeout");
        assert2::assert!(result.is_err());

        bootstrap.stop();
        controller.stop();
        slow.stop();
    }

    #[tokio::test]
    async fn custom_options_are_observable_on_bootstrap_reconnect() {
        let slow = ObservedAdminBroker::start(Duration::from_millis(300)).await;
        let live = ObservedAdminBroker::start(Duration::ZERO).await;
        let bootstrap = [slow.addr.to_string(), live.addr.to_string()];
        let mut admin = tokio::time::timeout(
            Duration::from_secs(1),
            AdminClient::connect_with_options(&bootstrap, custom_admin_options()),
        )
        .await
        .expect("custom initial timeout advances to next bootstrap")
        .unwrap();

        tokio::time::timeout(Duration::from_secs(1), admin.reconnect_bootstrap())
            .await
            .expect("custom reconnect timeout advances to next bootstrap")
            .unwrap();
        assert2::assert!(live.connections.load(Ordering::SeqCst) == 2);
        assert2::assert!(live.observed_custom_security_and_id());
        assert_custom_connect_timeout_is_stored(&admin);
        metadata_times_out_with_custom_request_timeout(&mut admin).await;

        slow.stop();
        live.stop();
    }

    #[test]
    fn existing_connectors_keep_admin_defaults() {
        let options = AdminClient::opts(None);

        assert2::assert!(options.client_id == "crabka-operator");
        assert2::assert!(options.connect_timeout == secs(5));
        assert2::assert!(options.request_timeout == secs(30));
        assert2::assert!(options.security.is_none());
    }
}
