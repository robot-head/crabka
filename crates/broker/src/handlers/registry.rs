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

    pub(crate) fn registered_api_keys(&self) -> impl Iterator<Item = ApiKeyCode> + '_ {
        self.table.keys().copied()
    }

    pub(crate) fn body_flexible(&self, api_key: ApiKeyCode, version: ApiVersion) -> bool {
        self.get(api_key)
            .is_some_and(|entry| entry.body_flexible(version))
    }
}

macro_rules! plain_dispatches {
    ($register_fn:ident; $(($api:ident, $request:ident, $handler:path)),+ $(,)?) => {
        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert2::assert!(registry.register(DispatchEntry::plain(
                        ApiKey::$api,
                        crabka_protocol::owned::$request::FLEXIBLE_MIN,
                        $handler,
                    )));
            )+
        }
    };
}

plain_dispatches!(register_plain_dispatches;
    (ApiVersions, api_versions_request, crate::handlers::api_versions::handle),
    (AddOffsetsToTxn, add_offsets_to_txn_request, crate::txn::handlers::add_offset_commits_to_txn::handle),
    (WriteTxnMarkers, write_txn_markers_request, crate::txn::handlers::write_txn_markers::handle),
    (FetchSnapshot, fetch_snapshot_request, crate::handlers::fetch_snapshot::handle),
    (ConsumerGroupDescribe, consumer_group_describe_request, crate::handlers::consumer_group_describe::handle),
    (AssignReplicasToDirs, assign_replicas_to_dirs_request, crate::handlers::assign_replicas_to_dirs::handle),
    (InitializeShareGroupState, initialize_share_group_state_request, crate::share_coordinator::handlers::initialize::handle),
    (ReadShareGroupState, read_share_group_state_request, crate::share_coordinator::handlers::read::handle),
    (WriteShareGroupState, write_share_group_state_request, crate::share_coordinator::handlers::write::handle),
    (DeleteShareGroupState, delete_share_group_state_request, crate::share_coordinator::handlers::delete::handle),
    (ReadShareGroupStateSummary, read_share_group_state_summary_request, crate::share_coordinator::handlers::read_summary::handle),
    (StreamsGroupDescribe, streams_group_describe_request, crate::handlers::streams_group_describe::handle),
);

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

macro_rules! context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api:ident, $request:ident, $handler:path)),+ $(,)?) => {
        $(context_adapter!($adapter, $handler);)+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert2::assert!(registry.register(DispatchEntry::context(
                        ApiKey::$api,
                        crabka_protocol::owned::$request::FLEXIBLE_MIN,
                        $adapter,
                    )));
            )+
        }
    };
}

macro_rules! decoded_context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api:ident, $request_mod:ident, $request_ty:ident, $handler:path)),+ $(,)?) => {
        $(
            fn $adapter<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                _correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(async move {
                    use crabka_protocol::Decode;

                    let mut cur = body;
                    let req = crabka_protocol::owned::$request_mod::$request_ty::decode(
                        &mut cur, version,
                    )?;
                    $handler(broker, req, ctx, version).await
                })
            }
        )+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert2::assert!(registry.register(DispatchEntry::decoded_context(
                        ApiKey::$api,
                        crabka_protocol::owned::$request_mod::FLEXIBLE_MIN,
                        $adapter,
                    )));
            )+
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

decoded_context_dispatches!(register_decoded_context_dispatches;
    (describe_acls_adapter, DescribeAcls, describe_acls_request, DescribeAclsRequest, crate::handlers::describe_acls::handle),
    (create_acls_adapter, CreateAcls, create_acls_request, CreateAclsRequest, crate::handlers::create_acls::handle),
    (delete_acls_adapter, DeleteAcls, delete_acls_request, DeleteAclsRequest, crate::handlers::delete_acls::handle),
    (elect_leaders_adapter, ElectLeaders, elect_leaders_request, ElectLeadersRequest, crate::handlers::elect_leaders::handle),
    (alter_partition_reassignments_adapter, AlterPartitionReassignments, alter_partition_reassignments_request, AlterPartitionReassignmentsRequest, crate::handlers::alter_partition_reassignments::handle),
    (list_partition_reassignments_adapter, ListPartitionReassignments, list_partition_reassignments_request, ListPartitionReassignmentsRequest, crate::handlers::list_partition_reassignments::handle),
    (describe_client_quotas_adapter, DescribeClientQuotas, describe_client_quotas_request, DescribeClientQuotasRequest, crate::handlers::describe_client_quotas::handle),
    (alter_client_quotas_adapter, AlterClientQuotas, alter_client_quotas_request, AlterClientQuotasRequest, crate::handlers::alter_client_quotas::handle),
    (describe_user_scram_credentials_adapter, DescribeUserScramCredentials, describe_user_scram_credentials_request, DescribeUserScramCredentialsRequest, crate::handlers::describe_user_scram_credentials::handle),
);

fn alter_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, ctx).await;
        crate::handlers::encode_response(&resp, version)
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
            &mut cur, version,
        )?;
        let resp = crate::handlers::update_features::handle(broker, req, version, ctx).await;
        crate::handlers::encode_response(&resp, version)
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

        use crabka_protocol::{
            Decode,
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
            return crate::handlers::encode_response(&resp, version);
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
        use crabka_protocol::Decode;

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
        crate::handlers::encode_response(&resp, version)
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
        use crabka_protocol::Decode;

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
        crate::handlers::encode_response(&resp, version)
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
        use crabka_protocol::Decode;

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
        crate::handlers::encode_response(&resp, version)
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
        use crabka_protocol::Decode;

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
        crate::handlers::encode_response(&resp, version)
    })
}

context_dispatches!(register_context_dispatches;
    (metadata_adapter, Metadata, metadata_request, crate::handlers::metadata::handle),
    (create_topics_adapter, CreateTopics, create_topics_request, crate::handlers::create_topics::handle),
    (delete_topics_adapter, DeleteTopics, delete_topics_request, crate::handlers::delete_topics::handle),
    (alter_configs_adapter, AlterConfigs, alter_configs_request, crate::handlers::alter_configs::handle),
    (incremental_alter_configs_adapter, IncrementalAlterConfigs, incremental_alter_configs_request, crate::handlers::incremental_alter_configs::handle),
    (delete_records_adapter, DeleteRecords, delete_records_request, crate::handlers::delete_records::handle),
    (create_partitions_adapter, CreatePartitions, create_partitions_request, crate::handlers::create_partitions::handle),
    (describe_groups_adapter, DescribeGroups, describe_groups_request, crate::handlers::describe_groups::handle),
    (list_groups_adapter, ListGroups, list_groups_request, crate::handlers::list_groups::handle),
    (share_group_describe_adapter, ShareGroupDescribe, share_group_describe_request, crate::handlers::share_group_describe::handle),
    (share_fetch_adapter, ShareFetch, share_fetch_request, crate::handlers::share_fetch::handle),
    (share_acknowledge_adapter, ShareAcknowledge, share_acknowledge_request, crate::handlers::share_acknowledge::handle),
    (describe_share_group_offsets_adapter, DescribeShareGroupOffsets, describe_share_group_offsets_request, crate::handlers::describe_share_group_offsets::handle),
    (alter_share_group_offsets_adapter, AlterShareGroupOffsets, alter_share_group_offsets_request, crate::handlers::alter_share_group_offsets::handle),
    (delete_share_group_offsets_adapter, DeleteShareGroupOffsets, delete_share_group_offsets_request, crate::handlers::delete_share_group_offsets::handle),
    (delete_groups_adapter, DeleteGroups, delete_groups_request, crate::handlers::delete_groups::handle),
    (join_group_adapter, JoinGroup, join_group_request, crate::handlers::join_group::handle),
    (offset_commit_adapter, OffsetCommit, offset_commit_request, crate::handlers::offset_commit::handle),
    (offset_fetch_adapter, OffsetFetch, offset_fetch_request, crate::handlers::offset_fetch::handle),
    (offset_delete_adapter, OffsetDelete, offset_delete_request, crate::handlers::offset_delete::handle),
    (describe_cluster_adapter, DescribeCluster, describe_cluster_request, crate::handlers::describe_cluster::handle),
    (describe_producers_adapter, DescribeProducers, describe_producers_request, crate::handlers::describe_producers::handle),
    (describe_transactions_adapter, DescribeTransactions, describe_transactions_request, crate::handlers::describe_transactions::handle),
    (list_transactions_adapter, ListTransactions, list_transactions_request, crate::handlers::list_transactions::handle),
    (unregister_broker_adapter, UnregisterBroker, unregister_broker_request, crate::handlers::unregister_broker::handle),
    (describe_topic_partitions_adapter, DescribeTopicPartitions, describe_topic_partitions_request, crate::handlers::describe_topic_partitions::handle),
    (list_config_resources_adapter, ListConfigResources, list_config_resources_request, crate::handlers::list_config_resources::handle),
    (describe_quorum_adapter, DescribeQuorum, describe_quorum_request, crate::handlers::describe_quorum::handle),
    (add_raft_voter_adapter, AddRaftVoter, add_raft_voter_request, crate::handlers::add_raft_voter::handle),
    (remove_raft_voter_adapter, RemoveRaftVoter, remove_raft_voter_request, crate::handlers::remove_raft_voter::handle),
    (update_raft_voter_adapter, UpdateRaftVoter, update_raft_voter_request, crate::handlers::update_raft_voter::handle),
    (alter_partition_adapter, AlterPartition, alter_partition_request, crate::handlers::alter_partition::handle),
    (broker_heartbeat_adapter, BrokerHeartbeat, broker_heartbeat_request, crate::handlers::broker_heartbeat::handle),
    (get_replica_log_info_adapter, GetReplicaLogInfo, get_replica_log_info_request, crate::handlers::get_replica_log_info::handle),
    (heartbeat_adapter, Heartbeat, heartbeat_request, crate::handlers::heartbeat::handle),
    (sync_group_adapter, SyncGroup, sync_group_request, crate::handlers::sync_group::handle),
    (leave_group_adapter, LeaveGroup, leave_group_request, crate::handlers::leave_group::handle),
    (consumer_group_heartbeat_adapter, ConsumerGroupHeartbeat, consumer_group_heartbeat_request, crate::handlers::consumer_group_heartbeat::handle),
    (share_group_heartbeat_adapter, ShareGroupHeartbeat, share_group_heartbeat_request, crate::handlers::share_group_heartbeat::handle),
    (streams_group_heartbeat_adapter, StreamsGroupHeartbeat, streams_group_heartbeat_request, crate::handlers::streams_group_heartbeat::handle),
    (find_coordinator_adapter, FindCoordinator, find_coordinator_request, crate::handlers::find_coordinator::handle),
    (list_offsets_adapter, ListOffsets, list_offsets_request, crate::handlers::list_offsets::handle),
    (offset_for_leader_epoch_adapter, OffsetForLeaderEpoch, offset_for_leader_epoch_request, crate::handlers::offset_for_leader_epoch::handle),
    (describe_configs_adapter, DescribeConfigs, describe_configs_request, crate::handlers::describe_configs::handle),
    (describe_log_dirs_adapter, DescribeLogDirs, describe_log_dirs_request, crate::handlers::describe_log_dirs::handle),
    (init_producer_id_adapter, InitProducerId, init_producer_id_request, crate::handlers::init_producer_id::handle),
    (add_partitions_to_txn_adapter, AddPartitionsToTxn, add_partitions_to_txn_request, crate::txn::handlers::add_partitions_to_txn::handle),
    (end_txn_adapter, EndTxn, end_txn_request, crate::txn::handlers::end_txn::handle),
    (txn_offset_commit_adapter, TxnOffsetCommit, txn_offset_commit_request, crate::txn::handlers::txn_offset_commit::handle),
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

    register_plain_dispatches(&mut registry);

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
    register_context_dispatches(&mut registry);
    register_decoded_context_dispatches(&mut registry);
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
    use std::collections::BTreeSet;

    use super::*;
    use crate::handlers;

    macro_rules! named_api_keys {
        ($($key:ident),+ $(,)?) => {
            [$( (stringify!($key), ApiKey::$key) ),+]
        };
    }

    #[test]
    fn registry_registers_plain_handlers() {
        let registry = build_registry();

        let api_versions = registry
            .get(ApiKey::ApiVersions as i16)
            .expect("ApiVersions");
        assert2::assert!(api_versions.is_plain());
        assert2::assert!(
            api_versions.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting
        );
        assert2::assert!(api_versions.body_flexible(3));
        assert2::assert!(!api_versions.body_flexible(2));

        for (_case, key) in [
            ("add offsets to transaction", 25),
            ("write transaction markers", 27),
            ("fetch snapshot", 59),
            ("consumer group heartbeat", 69),
            ("share group heartbeat", 73),
            ("initialize share group state", 83),
            ("read share group state", 84),
            ("write share group state", 85),
            ("delete share group state", 86),
            ("read share group state summary", 87),
            ("delete share group offsets", 89),
        ] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert2::assert!(entry.is_plain());
        }
    }

    #[test]
    fn registry_registers_raw_context_handlers() {
        let registry = build_registry();

        for (_case, api_key) in named_api_keys![
            Produce,
            Metadata,
            OffsetCommit,
            OffsetFetch,
            FindCoordinator,
            JoinGroup,
            Heartbeat,
            LeaveGroup,
            SyncGroup,
            DeleteGroups,
            ListOffsets,
            OffsetForLeaderEpoch,
            CreateTopics,
            DeleteTopics,
            AlterConfigs,
            IncrementalAlterConfigs,
            DeleteRecords,
            CreatePartitions,
            DescribeGroups,
            ListGroups,
            OffsetDelete,
            DescribeCluster,
            DescribeProducers,
            DescribeTransactions,
            ListTransactions,
            UnregisterBroker,
            DescribeTopicPartitions,
            ListConfigResources,
            DescribeQuorum,
            AddRaftVoter,
            RemoveRaftVoter,
            UpdateRaftVoter,
            AlterPartition,
            BrokerHeartbeat,
            GetReplicaLogInfo,
            ConsumerGroupHeartbeat,
            ShareGroupDescribe,
            ShareFetch,
            ShareAcknowledge,
            ShareGroupHeartbeat,
            StreamsGroupHeartbeat,
            DescribeShareGroupOffsets,
            AlterShareGroupOffsets,
            DeleteShareGroupOffsets,
            InitProducerId,
            AddPartitionsToTxn,
            EndTxn,
            TxnOffsetCommit,
        ] {
            let key = api_key as i16;
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert2::assert!(matches!(
                entry.kind(),
                DispatchKind::Context(_) | DispatchKind::Produce(_)
            ));
        }
    }

    #[test]
    fn registry_registers_telemetry_handlers() {
        let registry = build_registry();

        for (_case, key) in [("get telemetry subscriptions", 71), ("push telemetry", 72)] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert2::assert!(matches!(entry.kind(), DispatchKind::Telemetry(_)));
        }
    }

    #[test]
    fn registry_registers_decoded_context_handlers() {
        let registry = build_registry();

        for (_case, api_key) in named_api_keys![
            DescribeAcls,
            CreateAcls,
            DeleteAcls,
            ElectLeaders,
            AlterPartitionReassignments,
            ListPartitionReassignments,
            DescribeClientQuotas,
            AlterClientQuotas,
            DescribeUserScramCredentials,
            AlterUserScramCredentials,
            UpdateFeatures,
        ] {
            let key = api_key as i16;
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert2::assert!(matches!(
                entry.kind(),
                DispatchKind::DecodedContext(_) | DispatchKind::EncodedContext(_)
            ));
        }
    }

    #[test]
    fn registry_registers_auth_handlers() {
        let registry = build_registry();

        for (_case, key) in [
            ("alter replica log directories", 34),
            ("create delegation token", 38),
            ("renew delegation token", 39),
            ("expire delegation token", 40),
            ("describe delegation token", 41),
        ] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert2::assert!(matches!(entry.kind(), DispatchKind::Auth(_)));
        }
    }

    #[test]
    fn registry_reports_missing_keys() {
        let registry = build_registry();

        assert2::assert!(registry.get(9999).is_none());
    }

    #[test]
    fn registry_and_api_catalog_cover_same_api_keys() {
        let registry = build_registry();
        let registered: BTreeSet<ApiKeyCode> = registry.registered_api_keys().collect();
        let advertised: BTreeSet<ApiKeyCode> = crate::api_catalog::supported_apis()
            .into_iter()
            .map(|api| api.api_key)
            .collect();

        assert2::assert!(registered == advertised);
    }

    #[test]
    fn registry_body_flexible_matches_selected_schema_boundaries() {
        use crabka_protocol::owned;

        let registry = build_registry();
        let cases = [
            (
                "produce before flexible minimum",
                0,
                owned::produce_request::FLEXIBLE_MIN - 1,
                false,
            ),
            (
                "produce at flexible minimum",
                0,
                owned::produce_request::FLEXIBLE_MIN,
                true,
            ),
            (
                "fetch before flexible minimum",
                1,
                owned::fetch_request::FLEXIBLE_MIN - 1,
                false,
            ),
            (
                "fetch at flexible minimum",
                1,
                owned::fetch_request::FLEXIBLE_MIN,
                true,
            ),
            (
                "SASL authenticate before flexible minimum",
                36,
                owned::sasl_authenticate_request::FLEXIBLE_MIN - 1,
                false,
            ),
            (
                "SASL authenticate at flexible minimum",
                36,
                owned::sasl_authenticate_request::FLEXIBLE_MIN,
                true,
            ),
            ("non-flexible SASL handshake", 17, i16::MAX, false),
            ("unknown API", 999, 0, false),
        ];

        for (_case, api_key, version, want) in cases {
            assert2::assert!(registry.body_flexible(api_key, version) == want);
        }
    }

    #[test]
    fn plain_handler_pointer_matches_existing_api_versions_handler() {
        let registry = build_registry();
        let handler = registry
            .get_plain(ApiKey::ApiVersions as i16)
            .expect("plain ApiVersions handler");

        assert2::assert!(std::ptr::fn_addr_eq(
            handler,
            handlers::api_versions::handle as PlainHandler
        ));
    }
}
