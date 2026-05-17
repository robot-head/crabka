//! CRD type definitions. Each kind lives in its own submodule and is the
//! single source of truth for both the runtime types and the generated
//! CRD YAML manifest (see `gen_crds`).

pub mod kafka;
pub mod kafka_node_pool;

pub use kafka::{Kafka, KafkaCondition, KafkaSpec, KafkaStatus};
pub use kafka_node_pool::{
    KafkaNodePool, KafkaNodePoolSpec, KafkaNodePoolStatus, MetadataTemplate, NodeRole,
    PersistentClaimSpec, PodTemplate, Storage,
};
