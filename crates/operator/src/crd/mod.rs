//! CRD type definitions. Each kind lives in its own submodule and is the
//! single source of truth for both the runtime types and the generated
//! CRD YAML manifest (see `gen_crds`).

pub mod kafka;

pub use kafka::{Kafka, KafkaCondition, KafkaSpec, KafkaStatus};
