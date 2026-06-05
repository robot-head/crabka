//! Kafka-backed schema store — reads/writes from the `_schemas` compacted topic.

pub mod reader;
pub mod record;
pub mod topic;
pub mod writer;
