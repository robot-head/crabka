//! Broker API dispatch registry.

use bytes::Bytes;
use crabka_protocol::api_key::ApiKey;
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId, RequestContext, TelemetryContext},
};

pub(crate) type PlainHandler =
    fn(&Broker, ApiVersion, CorrelationId, &[u8]) -> BoxFuture<'static, Result<Bytes, BrokerError>>;

pub(crate) type ContextHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type ProduceHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    Bytes,
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type TelemetryHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a TelemetryContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type AuthHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a crate::network::auth::ConnectionAuth,
    &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestQuotaPolicy {
    ApplyFallbackAccounting,
    InlineExempt,
    SelfAccounted,
}

#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
    DecodedContext(ContextHandler),
    EncodedContext(ContextHandler),
    Auth(AuthHandler),
    Fetch,
    SaslMetadata,
}

#[derive(Clone, Copy)]
pub(crate) struct DispatchEntry {
    api_key: ApiKey,
    flexible_min: ApiVersion,
    quota_policy: RequestQuotaPolicy,
    kind: DispatchKind,
}

#[derive(Default)]
pub(crate) struct DispatchRegistry {
    table: std::collections::HashMap<ApiKeyCode, DispatchEntry>,
}

impl DispatchEntry {
    pub(crate) fn plain(api_key: ApiKey, flexible_min: ApiVersion, handler: PlainHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Plain(handler),
        }
    }

    pub(crate) fn context(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Context(handler),
        }
    }

    pub(crate) fn produce(flexible_min: ApiVersion, handler: ProduceHandler) -> Self {
        Self {
            api_key: ApiKey::Produce,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Produce(handler),
        }
    }

    pub(crate) fn telemetry(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: TelemetryHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Telemetry(handler),
        }
    }

    pub(crate) fn decoded_context(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::DecodedContext(handler),
        }
    }

    pub(crate) fn encoded_context(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::EncodedContext(handler),
        }
    }

    pub(crate) fn auth(api_key: ApiKey, flexible_min: ApiVersion, handler: AuthHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Auth(handler),
        }
    }

    pub(crate) fn fetch(flexible_min: ApiVersion) -> Self {
        Self {
            api_key: ApiKey::Fetch,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Fetch,
        }
    }

    pub(crate) fn sasl_metadata(api_key: ApiKey, flexible_min: ApiVersion) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::SaslMetadata,
        }
    }

    pub(crate) fn api_key(self) -> ApiKey {
        self.api_key
    }

    pub(crate) fn kind(self) -> DispatchKind {
        self.kind
    }

    pub(crate) fn quota_policy(self) -> RequestQuotaPolicy {
        self.quota_policy
    }

    pub(crate) fn body_flexible(self, version: ApiVersion) -> bool {
        self.flexible_min != i16::MAX && version >= self.flexible_min
    }

    pub(crate) fn is_plain(self) -> bool {
        matches!(self.kind, DispatchKind::Plain(_))
    }
}

impl DispatchRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&mut self, entry: DispatchEntry) -> bool {
        self.table.insert(entry.api_key as i16, entry).is_none()
    }

    pub(crate) fn get(&self, api_key: ApiKeyCode) -> Option<DispatchEntry> {
        self.table.get(&api_key).copied()
    }

    pub(crate) fn get_plain(&self, api_key: ApiKeyCode) -> Option<PlainHandler> {
        match self.get(api_key)?.kind {
            DispatchKind::Plain(handler) => Some(handler),
            _ => None,
        }
    }

    pub(crate) fn body_flexible(&self, api_key: ApiKeyCode, version: ApiVersion) -> bool {
        self.get(api_key)
            .is_some_and(|entry| entry.body_flexible(version))
    }
}

macro_rules! register_plain {
    ($registry:ident, $api:ident, $request:ident, $handler:ident) => {{
        $registry.register(DispatchEntry::plain(
            ApiKey::$api,
            crabka_protocol::owned::$request::FLEXIBLE_MIN,
            crate::handlers::$handler::handle,
        ));
    }};
}

macro_rules! context_adapter {
    ($adapter:ident, $handler:expr) => {
        fn $adapter<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a RequestContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(($handler)(broker, version, correlation_id, body, ctx))
        }
    };
}

macro_rules! telemetry_adapter {
    ($adapter:ident, $handler:expr) => {
        fn $adapter<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a TelemetryContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(($handler)(broker, version, correlation_id, body, ctx))
        }
    };
}

fn produce_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    body_bytes: Bytes,
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(crate::handlers::produce::handle(
        broker,
        version,
        correlation_id,
        body,
        body_bytes,
        ctx,
    ))
}

fn describe_acls_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::describe_acls_request::DescribeAclsRequest::decode(
            &mut cur, version,
        )?;
        crate::handlers::describe_acls::handle(broker, req, ctx, version).await
    })
}

fn create_acls_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::create_acls_request::CreateAclsRequest::decode(
            &mut cur, version,
        )?;
        crate::handlers::create_acls::handle(broker, req, ctx, version).await
    })
}

fn delete_acls_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::delete_acls_request::DeleteAclsRequest::decode(
            &mut cur, version,
        )?;
        crate::handlers::delete_acls::handle(broker, req, ctx, version).await
    })
}

fn elect_leaders_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::elect_leaders_request::ElectLeadersRequest::decode(
            &mut cur, version,
        )?;
        crate::handlers::elect_leaders::handle(broker, req, ctx, version).await
    })
}

fn alter_partition_reassignments_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::alter_partition_reassignments::handle(broker, req, ctx, version).await
    })
}

fn list_partition_reassignments_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::list_partition_reassignments::handle(broker, req, ctx, version).await
    })
}

fn describe_client_quotas_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::describe_client_quotas_request::DescribeClientQuotasRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::describe_client_quotas::handle(broker, req, ctx, version).await
    })
}

fn alter_client_quotas_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req =
            crabka_protocol::owned::alter_client_quotas_request::AlterClientQuotasRequest::decode(
                &mut cur, version,
            )?;
        crate::handlers::alter_client_quotas::handle(broker, req, ctx, version).await
    })
}

fn describe_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::describe_user_scram_credentials::handle(broker, req, ctx, version).await
    })
}

fn alter_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, ctx).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn update_features_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
            &mut cur, version,
        )?;
        let resp = crate::handlers::update_features::handle(broker, req, version, ctx).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn alter_replica_log_dirs_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use std::collections::BTreeMap;

        use bytes::BytesMut;
        use crabka_protocol::{
            Decode, Encode,
            owned::{
                alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
                alter_replica_log_dirs_response::{
                    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                    AlterReplicaLogDirsResponse,
                },
            },
        };

        let anonymous;
        let principal = if let Some(principal) = auth.principal() {
            principal
        } else {
            anonymous = crabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: crabka_security::AuthMethod::Anonymous,
                groups: vec![],
            };
            &anonymous
        };

        let image = broker.controller.current_image();
        let authorized = broker.config.authorizer.authorize(
            &*image,
            &crate::authorizer::AuthorizationRequest {
                principal,
                host: peer,
                resource_type: crabka_metadata::ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: crabka_metadata::AclOperation::Alter,
            },
        ) == crate::authorizer::AuthorizationResult::Allow;

        if !authorized {
            let mut cur = body;
            let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
            let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
                BTreeMap::new();
            for dir in req.dirs {
                for topic in dir.topics {
                    for partition_index in topic.partitions {
                        by_topic.entry(topic.name.clone()).or_default().push(
                            AlterReplicaLogDirPartitionResult {
                                partition_index,
                                error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
            let results = by_topic
                .into_iter()
                .map(|(topic_name, partitions)| AlterReplicaLogDirTopicResult {
                    topic_name,
                    partitions,
                    ..Default::default()
                })
                .collect();
            let resp = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        crate::handlers::alter_replica_log_dirs::handle(broker, version, correlation_id, body).await
    })
}

fn create_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::create_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_max_lifetime_ms,
            broker.config.delegation_token_default_renew_period_ms,
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn renew_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::renew_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_default_renew_period_ms,
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn expire_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::expire_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn describe_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::describe_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            peer,
            broker.config.authorizer.as_ref(),
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

context_adapter!(metadata_adapter, crate::handlers::metadata::handle);
context_adapter!(
    create_topics_adapter,
    crate::handlers::create_topics::handle
);
context_adapter!(
    delete_topics_adapter,
    crate::handlers::delete_topics::handle
);
context_adapter!(
    alter_configs_adapter,
    crate::handlers::alter_configs::handle
);
context_adapter!(
    incremental_alter_configs_adapter,
    crate::handlers::incremental_alter_configs::handle
);
context_adapter!(
    delete_records_adapter,
    crate::handlers::delete_records::handle
);
context_adapter!(
    create_partitions_adapter,
    crate::handlers::create_partitions::handle
);
context_adapter!(
    describe_groups_adapter,
    crate::handlers::describe_groups::handle
);
context_adapter!(list_groups_adapter, crate::handlers::list_groups::handle);
context_adapter!(
    share_group_describe_adapter,
    crate::handlers::share_group_describe::handle
);
context_adapter!(share_fetch_adapter, crate::handlers::share_fetch::handle);
context_adapter!(
    share_acknowledge_adapter,
    crate::handlers::share_acknowledge::handle
);
context_adapter!(
    describe_share_group_offsets_adapter,
    crate::handlers::describe_share_group_offsets::handle
);
context_adapter!(
    alter_share_group_offsets_adapter,
    crate::handlers::alter_share_group_offsets::handle
);
context_adapter!(
    delete_share_group_offsets_adapter,
    crate::handlers::delete_share_group_offsets::handle
);
context_adapter!(
    delete_groups_adapter,
    crate::handlers::delete_groups::handle
);
context_adapter!(join_group_adapter, crate::handlers::join_group::handle);
context_adapter!(
    offset_commit_adapter,
    crate::handlers::offset_commit::handle
);
context_adapter!(offset_fetch_adapter, crate::handlers::offset_fetch::handle);
context_adapter!(
    offset_delete_adapter,
    crate::handlers::offset_delete::handle
);
context_adapter!(
    describe_cluster_adapter,
    crate::handlers::describe_cluster::handle
);
context_adapter!(
    describe_producers_adapter,
    crate::handlers::describe_producers::handle
);
context_adapter!(
    describe_transactions_adapter,
    crate::handlers::describe_transactions::handle
);
context_adapter!(
    list_transactions_adapter,
    crate::handlers::list_transactions::handle
);
context_adapter!(
    unregister_broker_adapter,
    crate::handlers::unregister_broker::handle
);
context_adapter!(
    describe_topic_partitions_adapter,
    crate::handlers::describe_topic_partitions::handle
);
context_adapter!(
    list_config_resources_adapter,
    crate::handlers::list_config_resources::handle
);
context_adapter!(
    describe_quorum_adapter,
    crate::handlers::describe_quorum::handle
);
context_adapter!(
    add_raft_voter_adapter,
    crate::handlers::add_raft_voter::handle
);
context_adapter!(
    remove_raft_voter_adapter,
    crate::handlers::remove_raft_voter::handle
);
context_adapter!(
    update_raft_voter_adapter,
    crate::handlers::update_raft_voter::handle
);
context_adapter!(
    alter_partition_adapter,
    crate::handlers::alter_partition::handle
);
context_adapter!(
    broker_heartbeat_adapter,
    crate::handlers::broker_heartbeat::handle
);
context_adapter!(
    get_replica_log_info_adapter,
    crate::handlers::get_replica_log_info::handle
);
context_adapter!(heartbeat_adapter, crate::handlers::heartbeat::handle);
context_adapter!(sync_group_adapter, crate::handlers::sync_group::handle);
context_adapter!(leave_group_adapter, crate::handlers::leave_group::handle);
context_adapter!(
    consumer_group_heartbeat_adapter,
    crate::handlers::consumer_group_heartbeat::handle
);
context_adapter!(
    share_group_heartbeat_adapter,
    crate::handlers::share_group_heartbeat::handle
);
context_adapter!(
    streams_group_heartbeat_adapter,
    crate::handlers::streams_group_heartbeat::handle
);
context_adapter!(
    find_coordinator_adapter,
    crate::handlers::find_coordinator::handle
);
context_adapter!(list_offsets_adapter, crate::handlers::list_offsets::handle);
context_adapter!(
    offset_for_leader_epoch_adapter,
    crate::handlers::offset_for_leader_epoch::handle
);
context_adapter!(
    describe_configs_adapter,
    crate::handlers::describe_configs::handle
);
context_adapter!(
    describe_log_dirs_adapter,
    crate::handlers::describe_log_dirs::handle
);
context_adapter!(
    init_producer_id_adapter,
    crate::handlers::init_producer_id::handle
);
context_adapter!(
    add_partitions_to_txn_adapter,
    crate::txn::handlers::add_partitions_to_txn::handle
);
context_adapter!(end_txn_adapter, crate::txn::handlers::end_txn::handle);
context_adapter!(
    txn_offset_commit_adapter,
    crate::txn::handlers::txn_offset_commit::handle
);

telemetry_adapter!(
    get_telemetry_subscriptions_adapter,
    crate::handlers::get_telemetry_subscriptions::handle
);
telemetry_adapter!(
    push_telemetry_adapter,
    crate::handlers::push_telemetry::handle
);

#[allow(clippy::too_many_lines)]
pub(crate) fn build_registry() -> DispatchRegistry {
    let mut registry = DispatchRegistry::new();

    register_plain!(registry, ApiVersions, api_versions_request, api_versions);
    registry.register(DispatchEntry::plain(
        ApiKey::AddOffsetsToTxn,
        crabka_protocol::owned::add_offsets_to_txn_request::FLEXIBLE_MIN,
        crate::txn::handlers::add_offset_commits_to_txn::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::WriteTxnMarkers,
        crabka_protocol::owned::write_txn_markers_request::FLEXIBLE_MIN,
        crate::txn::handlers::write_txn_markers::handle,
    ));
    register_plain!(
        registry,
        FetchSnapshot,
        fetch_snapshot_request,
        fetch_snapshot
    );
    register_plain!(
        registry,
        ConsumerGroupDescribe,
        consumer_group_describe_request,
        consumer_group_describe
    );
    register_plain!(
        registry,
        AssignReplicasToDirs,
        assign_replicas_to_dirs_request,
        assign_replicas_to_dirs
    );
    registry.register(DispatchEntry::plain(
        ApiKey::InitializeShareGroupState,
        crabka_protocol::owned::initialize_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::initialize::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::ReadShareGroupState,
        crabka_protocol::owned::read_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::read::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::WriteShareGroupState,
        crabka_protocol::owned::write_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::write::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::DeleteShareGroupState,
        crabka_protocol::owned::delete_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::delete::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::ReadShareGroupStateSummary,
        crabka_protocol::owned::read_share_group_state_summary_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::read_summary::handle,
    ));
    register_plain!(
        registry,
        StreamsGroupDescribe,
        streams_group_describe_request,
        streams_group_describe
    );

    registry.register(DispatchEntry::produce(
        crabka_protocol::owned::produce_request::FLEXIBLE_MIN,
        produce_adapter,
    ));
    registry.register(DispatchEntry::fetch(
        crabka_protocol::owned::fetch_request::FLEXIBLE_MIN,
    ));
    registry.register(DispatchEntry::sasl_metadata(
        ApiKey::SaslHandshake,
        i16::MAX,
    ));
    registry.register(DispatchEntry::sasl_metadata(
        ApiKey::SaslAuthenticate,
        crabka_protocol::owned::sasl_authenticate_request::FLEXIBLE_MIN,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::Metadata,
        crabka_protocol::owned::metadata_request::FLEXIBLE_MIN,
        metadata_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::CreateTopics,
        crabka_protocol::owned::create_topics_request::FLEXIBLE_MIN,
        create_topics_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DeleteTopics,
        crabka_protocol::owned::delete_topics_request::FLEXIBLE_MIN,
        delete_topics_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::AlterConfigs,
        crabka_protocol::owned::alter_configs_request::FLEXIBLE_MIN,
        alter_configs_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::IncrementalAlterConfigs,
        crabka_protocol::owned::incremental_alter_configs_request::FLEXIBLE_MIN,
        incremental_alter_configs_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DeleteRecords,
        crabka_protocol::owned::delete_records_request::FLEXIBLE_MIN,
        delete_records_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::CreatePartitions,
        crabka_protocol::owned::create_partitions_request::FLEXIBLE_MIN,
        create_partitions_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeGroups,
        crabka_protocol::owned::describe_groups_request::FLEXIBLE_MIN,
        describe_groups_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ListGroups,
        crabka_protocol::owned::list_groups_request::FLEXIBLE_MIN,
        list_groups_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ShareGroupDescribe,
        crabka_protocol::owned::share_group_describe_request::FLEXIBLE_MIN,
        share_group_describe_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ShareFetch,
        crabka_protocol::owned::share_fetch_request::FLEXIBLE_MIN,
        share_fetch_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ShareAcknowledge,
        crabka_protocol::owned::share_acknowledge_request::FLEXIBLE_MIN,
        share_acknowledge_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeShareGroupOffsets,
        crabka_protocol::owned::describe_share_group_offsets_request::FLEXIBLE_MIN,
        describe_share_group_offsets_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::AlterShareGroupOffsets,
        crabka_protocol::owned::alter_share_group_offsets_request::FLEXIBLE_MIN,
        alter_share_group_offsets_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DeleteShareGroupOffsets,
        crabka_protocol::owned::delete_share_group_offsets_request::FLEXIBLE_MIN,
        delete_share_group_offsets_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DeleteGroups,
        crabka_protocol::owned::delete_groups_request::FLEXIBLE_MIN,
        delete_groups_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::JoinGroup,
        crabka_protocol::owned::join_group_request::FLEXIBLE_MIN,
        join_group_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::OffsetCommit,
        crabka_protocol::owned::offset_commit_request::FLEXIBLE_MIN,
        offset_commit_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::OffsetFetch,
        crabka_protocol::owned::offset_fetch_request::FLEXIBLE_MIN,
        offset_fetch_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::OffsetDelete,
        crabka_protocol::owned::offset_delete_request::FLEXIBLE_MIN,
        offset_delete_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeCluster,
        crabka_protocol::owned::describe_cluster_request::FLEXIBLE_MIN,
        describe_cluster_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeProducers,
        crabka_protocol::owned::describe_producers_request::FLEXIBLE_MIN,
        describe_producers_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeTransactions,
        crabka_protocol::owned::describe_transactions_request::FLEXIBLE_MIN,
        describe_transactions_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ListTransactions,
        crabka_protocol::owned::list_transactions_request::FLEXIBLE_MIN,
        list_transactions_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::UnregisterBroker,
        crabka_protocol::owned::unregister_broker_request::FLEXIBLE_MIN,
        unregister_broker_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeTopicPartitions,
        crabka_protocol::owned::describe_topic_partitions_request::FLEXIBLE_MIN,
        describe_topic_partitions_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ListConfigResources,
        crabka_protocol::owned::list_config_resources_request::FLEXIBLE_MIN,
        list_config_resources_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeQuorum,
        crabka_protocol::owned::describe_quorum_request::FLEXIBLE_MIN,
        describe_quorum_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::AddRaftVoter,
        crabka_protocol::owned::add_raft_voter_request::FLEXIBLE_MIN,
        add_raft_voter_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::RemoveRaftVoter,
        crabka_protocol::owned::remove_raft_voter_request::FLEXIBLE_MIN,
        remove_raft_voter_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::UpdateRaftVoter,
        crabka_protocol::owned::update_raft_voter_request::FLEXIBLE_MIN,
        update_raft_voter_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::AlterPartition,
        crabka_protocol::owned::alter_partition_request::FLEXIBLE_MIN,
        alter_partition_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::BrokerHeartbeat,
        crabka_protocol::owned::broker_heartbeat_request::FLEXIBLE_MIN,
        broker_heartbeat_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::GetReplicaLogInfo,
        crabka_protocol::owned::get_replica_log_info_request::FLEXIBLE_MIN,
        get_replica_log_info_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::Heartbeat,
        crabka_protocol::owned::heartbeat_request::FLEXIBLE_MIN,
        heartbeat_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::SyncGroup,
        crabka_protocol::owned::sync_group_request::FLEXIBLE_MIN,
        sync_group_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::LeaveGroup,
        crabka_protocol::owned::leave_group_request::FLEXIBLE_MIN,
        leave_group_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ConsumerGroupHeartbeat,
        crabka_protocol::owned::consumer_group_heartbeat_request::FLEXIBLE_MIN,
        consumer_group_heartbeat_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ShareGroupHeartbeat,
        crabka_protocol::owned::share_group_heartbeat_request::FLEXIBLE_MIN,
        share_group_heartbeat_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::StreamsGroupHeartbeat,
        crabka_protocol::owned::streams_group_heartbeat_request::FLEXIBLE_MIN,
        streams_group_heartbeat_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::FindCoordinator,
        crabka_protocol::owned::find_coordinator_request::FLEXIBLE_MIN,
        find_coordinator_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::ListOffsets,
        crabka_protocol::owned::list_offsets_request::FLEXIBLE_MIN,
        list_offsets_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::OffsetForLeaderEpoch,
        crabka_protocol::owned::offset_for_leader_epoch_request::FLEXIBLE_MIN,
        offset_for_leader_epoch_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeConfigs,
        crabka_protocol::owned::describe_configs_request::FLEXIBLE_MIN,
        describe_configs_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::DescribeLogDirs,
        crabka_protocol::owned::describe_log_dirs_request::FLEXIBLE_MIN,
        describe_log_dirs_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::DescribeAcls,
        crabka_protocol::owned::describe_acls_request::FLEXIBLE_MIN,
        describe_acls_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::CreateAcls,
        crabka_protocol::owned::create_acls_request::FLEXIBLE_MIN,
        create_acls_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::DeleteAcls,
        crabka_protocol::owned::delete_acls_request::FLEXIBLE_MIN,
        delete_acls_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::ElectLeaders,
        crabka_protocol::owned::elect_leaders_request::FLEXIBLE_MIN,
        elect_leaders_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::AlterPartitionReassignments,
        crabka_protocol::owned::alter_partition_reassignments_request::FLEXIBLE_MIN,
        alter_partition_reassignments_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::ListPartitionReassignments,
        crabka_protocol::owned::list_partition_reassignments_request::FLEXIBLE_MIN,
        list_partition_reassignments_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::DescribeClientQuotas,
        crabka_protocol::owned::describe_client_quotas_request::FLEXIBLE_MIN,
        describe_client_quotas_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::AlterClientQuotas,
        crabka_protocol::owned::alter_client_quotas_request::FLEXIBLE_MIN,
        alter_client_quotas_adapter,
    ));
    registry.register(DispatchEntry::decoded_context(
        ApiKey::DescribeUserScramCredentials,
        crabka_protocol::owned::describe_user_scram_credentials_request::FLEXIBLE_MIN,
        describe_user_scram_credentials_adapter,
    ));
    registry.register(DispatchEntry::encoded_context(
        ApiKey::AlterUserScramCredentials,
        crabka_protocol::owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        alter_user_scram_credentials_adapter,
    ));
    registry.register(DispatchEntry::encoded_context(
        ApiKey::UpdateFeatures,
        crabka_protocol::owned::update_features_request::FLEXIBLE_MIN,
        update_features_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::AlterReplicaLogDirs,
        crabka_protocol::owned::alter_replica_log_dirs_request::FLEXIBLE_MIN,
        alter_replica_log_dirs_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::CreateDelegationToken,
        crabka_protocol::owned::create_delegation_token_request::FLEXIBLE_MIN,
        create_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::RenewDelegationToken,
        crabka_protocol::owned::renew_delegation_token_request::FLEXIBLE_MIN,
        renew_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::ExpireDelegationToken,
        crabka_protocol::owned::expire_delegation_token_request::FLEXIBLE_MIN,
        expire_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::DescribeDelegationToken,
        crabka_protocol::owned::describe_delegation_token_request::FLEXIBLE_MIN,
        describe_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::InitProducerId,
        crabka_protocol::owned::init_producer_id_request::FLEXIBLE_MIN,
        init_producer_id_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::AddPartitionsToTxn,
        crabka_protocol::owned::add_partitions_to_txn_request::FLEXIBLE_MIN,
        add_partitions_to_txn_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::EndTxn,
        crabka_protocol::owned::end_txn_request::FLEXIBLE_MIN,
        end_txn_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::TxnOffsetCommit,
        crabka_protocol::owned::txn_offset_commit_request::FLEXIBLE_MIN,
        txn_offset_commit_adapter,
    ));
    registry.register(DispatchEntry::telemetry(
        ApiKey::GetTelemetrySubscriptions,
        crabka_protocol::owned::get_telemetry_subscriptions_request::FLEXIBLE_MIN,
        get_telemetry_subscriptions_adapter,
    ));
    registry.register(DispatchEntry::telemetry(
        ApiKey::PushTelemetry,
        crabka_protocol::owned::push_telemetry_request::FLEXIBLE_MIN,
        push_telemetry_adapter,
    ));

    registry
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers;

    #[test]
    fn registry_registers_plain_handlers() {
        let registry = build_registry();

        let api_versions = registry
            .get(ApiKey::ApiVersions as i16)
            .expect("ApiVersions");
        assert!(api_versions.is_plain());
        assert!(api_versions.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting);
        assert!(api_versions.body_flexible(3));
        assert!(!api_versions.body_flexible(2));

        for key in [25, 27, 59, 69, 73, 83, 84, 85, 86, 87, 89] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(entry.is_plain(), "api_key {key}");
        }
    }

    #[test]
    fn registry_registers_raw_context_handlers() {
        let registry = build_registry();

        for key in [
            0, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 24, 26, 28, 32, 33, 35, 37,
            42, 44, 47, 55, 56, 60, 61, 63, 64, 65, 66, 68, 74, 75, 76, 77, 78, 79, 80, 81, 82, 88,
            90, 91, 92, 93,
        ] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(
                    entry.kind(),
                    DispatchKind::Context(_) | DispatchKind::Produce(_)
                ),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_registers_telemetry_handlers() {
        let registry = build_registry();

        for key in [71, 72] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(entry.kind(), DispatchKind::Telemetry(_)),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_registers_decoded_context_handlers() {
        let registry = build_registry();

        for key in [29, 30, 31, 43, 45, 46, 48, 49, 50, 51, 57] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(
                    entry.kind(),
                    DispatchKind::DecodedContext(_) | DispatchKind::EncodedContext(_)
                ),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_registers_auth_handlers() {
        let registry = build_registry();

        for key in [34, 38, 39, 40, 41] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(entry.kind(), DispatchKind::Auth(_)),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_reports_missing_keys() {
        let registry = build_registry();

        assert!(registry.get(9999).is_none());
    }

    #[test]
    fn registry_body_flexible_matches_selected_schema_boundaries() {
        use crabka_protocol::owned;

        let registry = build_registry();
        let cases = [
            (0, owned::produce_request::FLEXIBLE_MIN - 1, false),
            (0, owned::produce_request::FLEXIBLE_MIN, true),
            (1, owned::fetch_request::FLEXIBLE_MIN - 1, false),
            (1, owned::fetch_request::FLEXIBLE_MIN, true),
            (
                36,
                owned::sasl_authenticate_request::FLEXIBLE_MIN - 1,
                false,
            ),
            (36, owned::sasl_authenticate_request::FLEXIBLE_MIN, true),
            (17, i16::MAX, false),
            (999, 0, false),
        ];

        for (api_key, version, want) in cases {
            assert!(
                registry.body_flexible(api_key, version) == want,
                "api_key {api_key} version {version}"
            );
        }
    }

    #[test]
    fn plain_handler_pointer_matches_existing_api_versions_handler() {
        let registry = build_registry();
        let handler = registry
            .get_plain(ApiKey::ApiVersions as i16)
            .expect("plain ApiVersions handler");

        assert!(std::ptr::fn_addr_eq(
            handler,
            handlers::api_versions::handle as PlainHandler
        ));
    }
}
