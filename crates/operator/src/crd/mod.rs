//! CRD type definitions. Each kind lives in its own submodule and is the
//! single source of truth for both the runtime types and the generated
//! CRD YAML manifest (see `gen_crds`).

pub mod ca;
pub mod gres;
pub mod gres_tenant;
pub mod grpc_gateway;
pub mod kafka;
pub mod kafka_node_pool;
pub mod listener;
pub mod logging;
pub mod metrics;
pub mod network_policy;
pub mod rebalance;
pub mod schema_registry;
pub mod topic;
pub mod user;

pub use ca::{CertificateAuthority, CertificateAuthorityStatus};
pub use gres::{
    Gres, GresBalancerGoal, GresBalancerGoals, GresBalancerOperationKind, GresBalancerPlanSnapshot,
    GresBalancerRegistryLayout, GresBalancerSpec, GresBalancerStatus, GresBalancerThresholds,
    GresSpec, GresStatus, PgdogSpec, SecretKeyRef, SecretRef, TenantDefaults,
};
pub use gres_tenant::{
    GresTenant, GresTenantRangeKey, GresTenantRangeSpec, GresTenantSpec, GresTenantStatus,
};
pub use grpc_gateway::{KafkaGrpcGateway, KafkaGrpcGatewaySpec, KafkaGrpcGatewayStatus};
pub use kafka::{
    Authorization, BrokerTuning, InterBrokerKerberos, Kafka, KafkaCondition, KafkaSpec,
    KafkaStatus, Krb5ConfSecretRef, OpaAuthorization, SimpleAuthorization,
};
pub use kafka_node_pool::{
    JbodSpec, JbodVolume, KafkaNodePool, KafkaNodePoolSpec, KafkaNodePoolStatus, MetadataTemplate,
    NodeRole, PersistentClaimSpec, PodTemplate, Storage,
};
pub use listener::*;
pub use logging::{ConfigMapKeyRef, ExternalLoggingSource, Logging, LoggingType};
pub use metrics::{MetricsConfig, MetricsType, PodMonitorSpec, ServiceMonitorSpec};
pub use network_policy::{NetworkPolicyPeer, NetworkPolicySpec};
pub use rebalance::{KafkaRebalance, KafkaRebalanceSpec, KafkaRebalanceStatus, OptimizationResult};
pub use schema_registry::{
    BasicAuthn, BearerAuthn, BearerMode, CertManagerIssuerRef, KafkaClientSasl, KafkaClientTls,
    SchemaRegistry, SchemaRegistryAuthn, SchemaRegistryAuthz, SchemaRegistryHealthChecks,
    SchemaRegistryKafkaClient, SchemaRegistryRuntime, SchemaRegistrySpec, SchemaRegistryStatus,
    SchemaRegistryTls, TlsClientAuth,
};
pub use topic::{KafkaTopic, KafkaTopicSpec, KafkaTopicStatus};
// The cluster-level `Authorization` / `SimpleAuthorization` on
// `KafkaSpec` (above) take the unqualified re-export names. The per-user
// counterparts on `KafkaUserSpec` are re-exported with a `KafkaUser`
// prefix to disambiguate — same role (drives ACL authorization) but at a
// different scope (one user vs the whole cluster) and with a different
// payload (per-user ACL rules vs cluster super-user list).
pub use user::{
    AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule, Authentication,
    Authorization as KafkaUserAuthorization, DelegationTokenAuth, KafkaUser, KafkaUserQuotas,
    KafkaUserSpec, KafkaUserStatus, ScramSha512Auth,
    SimpleAuthorization as KafkaUserSimpleAuthorization,
};
