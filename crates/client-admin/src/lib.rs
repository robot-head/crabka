//! Admin client for Crabka operators and control-plane services.
//!
//! The client targets the active controller and retries selected RPCs on a
//! refreshed controller connection when the broker returns `NOT_CONTROLLER`.
//! It supports plaintext by default and the same client-side TLS/SASL security
//! surface as [`crabka_client_core`] via [`AdminClient::connect_secured`].
//!
//! Built on `crabka_client_core::Connection`'s typed
//! `send::<R: ProtocolRequest>` so request-version negotiation is
//! automatic via the `ApiVersionTable` populated at connect time. The public
//! modules cover topic CRUD, partition expansion, config changes, SCRAM user
//! credentials, ACLs, quotas, delegation tokens, and log-dir inspection.

use std::time::Duration;

use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use thiserror::Error;

pub mod configs;
pub mod delegation_tokens;
pub mod groups;
pub mod log_dirs;
pub mod quotas;
pub mod topics;
pub mod users;

pub use configs::{AlterConfigsOutcome, IncrementalAlterOp, TopicConfigOverrides};
pub use log_dirs::{AlterReplicaLogDirOutcome, LogDirInfo, LogDirPartitionInfo, LogDirTopicInfo};
pub use quotas::{QuotaOp, UserQuotaConfig, diff_user_quotas};
pub use topics::{
    CreatePartitionsOp, CreatePartitionsOutcome, CreateTopicOutcome, CreateTopicSpec,
    DeleteRecordsOp, DeleteRecordsOutcome, DeleteTopicOutcome, TopicMetadata, TopicMetadataEntry,
};
pub use users::{
    AclEntry, AclEntryFilter, AclOperation, CreateAclOutcome, DEFAULT_SCRAM_ITERATIONS,
    DeleteAclFilterOutcome, PatternType, PermissionType, ResourceType, ScramDeletion,
    ScramUpsertion, ScramUserOutcome, UserScramCredential, UserScramCredentials,
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
    async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        timeout_ms: i32,
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
        max_lifetime_ms: i64,
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
    async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        timeout_ms: i32,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError> {
        AdminClient::delete_records(self, ops, timeout_ms).await
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
        max_lifetime_ms: i64,
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
            max_lifetime_ms,
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

/// Split `"Type:Name"` (default type `User`) into a `KafkaPrincipal`.
/// Empty input yields `None` so the create path doesn't manufacture a
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

/// Short-lived admin client targeting one cluster's controller.
/// Optionally negotiates TLS/SASL via [`AdminClient::connect_secured`].
pub struct AdminClient {
    pub(crate) conn: Connection,
    bootstrap_addrs: Vec<String>,
    /// Full connection template carried forward so reconnects preserve
    /// caller-supplied identity, security, and timeouts.
    options: ConnectionOptions,
}

impl AdminClient {
    /// Build the per-connect options for `client_id="crabka-operator"`,
    /// carrying the supplied security policy.
    fn opts(security: Option<crabka_client_core::security::ClientSecurity>) -> ConnectionOptions {
        ConnectionOptions {
            dns_timeout: crabka_client_core::ClientDnsTimeout::default(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            client_id: "crabka-operator".to_string(),
            security: security.map(Box::new),
        }
    }

    /// Connect, applying optional client security. `None` = plaintext
    /// (identical to [`AdminClient::connect`]).
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

    /// Connect using a complete connection-options template.
    ///
    /// # Errors
    /// Returns `AdminError::Connect { tried }` if no bootstrap address
    /// accepted the connection.
    pub async fn connect_with_options(
        bootstrap_addrs: &[String],
        options: ConnectionOptions,
    ) -> Result<Self, AdminError> {
        for host_port in bootstrap_addrs {
            match Self::connect_one(host_port, options.clone()).await {
                Ok(conn) => {
                    return Ok(Self {
                        conn,
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
                }
            }
        }
        Err(AdminError::Connect {
            tried: bootstrap_addrs.len(),
        })
    }

    /// Try each bootstrap address in order. Each entry is `host:port`;
    /// DNS is resolved via `tokio::net::lookup_host`. First successful
    /// connect wins. Returns `AdminError::Connect { tried }` if none
    /// responded. Plaintext; see [`AdminClient::connect_secured`].
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn connect(bootstrap_addrs: &[String]) -> Result<Self, AdminError> {
        Self::connect_secured(bootstrap_addrs, None).await
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
        Connection::connect_with_options(addr, opts)
            .await
            .map_err(AdminError::from)
    }

    /// Replace the underlying connection. Used internally by the
    /// `NOT_CONTROLLER` retry path to reconnect to the current controller.
    pub(crate) async fn reconnect(&mut self, host_port: &str) -> Result<(), AdminError> {
        let opts = self.options.clone();
        self.conn = Self::connect_one(host_port, opts).await?;
        Ok(())
    }

    pub(crate) async fn reconnect_bootstrap(&mut self) -> Result<(), AdminError> {
        let opts = self.options.clone();
        for host_port in &self.bootstrap_addrs {
            match Self::connect_one(host_port, opts.clone()).await {
                Ok(conn) => {
                    self.conn = conn;
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
        31 => "CLUSTER_AUTHORIZATION_FAILED",
        33 => "UNSUPPORTED_SASL_MECHANISM",
        35 => "UNSUPPORTED_VERSION",
        36 => "TOPIC_ALREADY_EXISTS",
        37 => "INVALID_PARTITIONS",
        38 => "INVALID_REPLICATION_FACTOR",
        39 => "INVALID_REPLICA_ASSIGNMENT",
        40 => "INVALID_CONFIG",
        41 => "NOT_CONTROLLER",
        66 => "DELEGATION_TOKEN_EXPIRED",
        83 => "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        84 => "ELECTION_NOT_NEEDED",
        91 => "RESOURCE_NOT_FOUND",
        92 => "DUPLICATE_RESOURCE",
        93 => "UNACCEPTABLE_CREDENTIAL",
        107 => "INELIGIBLE_REPLICA",
        87 => "REASSIGNMENT_IN_PROGRESS",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::{BufMut, BytesMut};
    use crabka_client_core::security::{ClientSecurity, SaslCredentials};
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request, sasl_authenticate_request,
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

    fn custom_admin_options() -> ConnectionOptions {
        ConnectionOptions {
            dns_timeout: crabka_client_core::ClientDnsTimeout::default(),
            client_id: "custom-admin".into(),
            connect_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_millis(25),
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
        let result = tokio::time::timeout(Duration::from_millis(500), admin.metadata(&[]))
            .await
            .expect("custom request timeout fires");
        assert2::assert!(result.is_err());
    }

    fn assert_custom_connect_timeout_is_stored(admin: &AdminClient) {
        assert2::assert!(admin.options.connect_timeout == Duration::from_millis(100));
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
        assert2::assert!(options.connect_timeout == Duration::from_secs(5));
        assert2::assert!(options.request_timeout == Duration::from_secs(30));
        assert2::assert!(options.security.is_none());
    }
}
