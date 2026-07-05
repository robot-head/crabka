//! TCP listener, per-connection task, and Kafka framing helpers.

pub(crate) mod auth;
/// Outbound inter-broker client (TLS + SASL). Public so peer crates
/// inside this workspace can `use crabka_broker::network::client::*`.
pub mod client;
pub(crate) mod codec;
pub(crate) mod dispatch;
/// Zero-copy fetch response write-plan + vectored/sendfile drain.
pub(crate) mod fetch_writer;
/// Linux kTLS support probe (Increment F): run once at startup to decide
/// whether TLS fetch connections route through kernel-offloaded sendfile.
pub(crate) mod ktls_probe;
pub(crate) mod listener;
pub(crate) mod request;
