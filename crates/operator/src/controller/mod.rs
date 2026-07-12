//! Controllers (reconcilers) for Crabka CRDs. Each kind lives in its own
//! submodule and shares helpers via `common` (cluster-level rendering,
//! SSA helpers, label / owner-ref builders, status derivation).

pub mod cluster_ca;
pub mod common;
pub mod gres;
pub mod gres_split_operation;
pub mod gres_tenant;
pub mod grpc_gateway;
pub mod kafka;
pub mod kafka_node_pool;
pub(crate) mod listeners;
pub(crate) mod logging;
pub(crate) mod metrics;
pub(crate) mod network_policy;
pub mod rebalance;
pub mod schema_registry;
pub mod topic;
pub mod user;
pub mod user_delegation_token;
pub mod user_tls;
