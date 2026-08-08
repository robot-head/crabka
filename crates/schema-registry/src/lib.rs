//! Confluent Schema Registry-compatible REST service for Crabka.
//!
//! This is a standalone binary and a Kafka *client* of a Crabka broker. State
//! lives in the `_schemas` compacted topic.
//!
//! ## Runtime configuration
//!
//! ```no_run
//! use crabka_schema_registry::config::{RegistryConfig, SecurityConfig};
//!
//! let config = RegistryConfig {
//!     bootstrap: "localhost:9092".into(),
//!     schemas_topic: "_schemas".into(),
//!     schemas_topic_rf: 3,
//!     client_id: "schema-registry-1".into(),
//!     advertised_url: "http://schema-registry-1:8081".into(),
//!     group_id: "schema-registry".into(),
//!     leader_eligibility: true,
//!     runtime: Default::default(),
//!     security: SecurityConfig::default(),
//! };
//!
//! assert_eq!(config.schemas_topic, "_schemas");
//! ```
//!
//! ## Compatibility checks
//!
//! ```no_run
//! use crabka_schema_registry::format::{self, SchemaType};
//!
//! let prior = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;
//! let next = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"total","type":["null","double"],"default":null}]}"#;
//!
//! assert!(format::check(SchemaType::Avro, next, prior, &[], &[]).is_ok());
//! ```

pub mod auth;
pub mod authz;
pub mod cli;
pub mod compat;
pub mod config;
pub mod config_value;
pub mod election;
pub mod error;
pub mod format;
pub mod ids;
pub mod kafkastore;
pub mod rest;
pub mod store;
