//! CRD type definitions. Each kind lives in its own submodule and is the
//! single source of truth for both the runtime types and the generated
//! CRD YAML manifest (see `gen_crds`).

pub mod ca;
pub mod kafka;
pub mod kafka_node_pool;
pub mod listener;
pub mod metrics;
pub mod network_policy;
pub mod rebalance;
pub mod topic;
pub mod user;

pub use ca::{CertificateAuthority, CertificateAuthorityStatus};
pub use kafka::{Kafka, KafkaCondition, KafkaSpec, KafkaStatus};
pub use kafka_node_pool::{
    KafkaNodePool, KafkaNodePoolSpec, KafkaNodePoolStatus, MetadataTemplate, NodeRole,
    PersistentClaimSpec, PodTemplate, Storage,
};
pub use listener::*;
pub use metrics::{MetricsConfig, MetricsType, PodMonitorSpec, ServiceMonitorSpec};
pub use network_policy::{NetworkPolicyPeer, NetworkPolicySpec};
pub use rebalance::{KafkaRebalance, KafkaRebalanceSpec, KafkaRebalanceStatus, OptimizationResult};
pub use topic::{KafkaTopic, KafkaTopicSpec, KafkaTopicStatus};
pub use user::{
    AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule, Authentication,
    Authorization, KafkaUser, KafkaUserQuotas, KafkaUserSpec, KafkaUserStatus, ScramSha512Auth,
    SimpleAuthorization,
};
