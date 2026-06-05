//! Confluent Schema Registry-compatible REST service for Crabka.
//!
//! Standalone binary; a Kafka *client* of a Crabka broker. State lives in the
//! `_schemas` compacted topic. See
//! `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md`.

pub mod compat;
pub mod config;
pub mod error;
pub mod format;
pub mod kafkastore;
pub mod rest;
pub mod store;
