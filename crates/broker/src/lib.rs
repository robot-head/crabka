//! Single-node Apache Kafka-compatible broker (MVP).

#![doc(html_root_url = "https://docs.rs/crabka-broker/0.0.0")]

mod codes;
mod config;
mod error;
mod log_dir;
mod metadata;
mod partition;
mod partition_writer;

pub use config::BrokerConfig;
pub use error::BrokerError;
