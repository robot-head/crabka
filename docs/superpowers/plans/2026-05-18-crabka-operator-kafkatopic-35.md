# Crabka Operator Slice 35 — `KafkaTopic` CRD + `crates/client-admin`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`. Per CLAUDE.md, dispatch tasks within a batch in parallel; sequential between batches. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add the `KafkaTopic` CRD with unidirectional reconcile (CRD wins) plus a new workspace crate `crates/client-admin` that wraps `crates/client-core`'s typed `Connection::send<R>` for the 6 admin RPCs slice 35 needs.

**Spec:** [`docs/superpowers/specs/2026-05-18-crabka-operator-kafkatopic-35-design.md`](../specs/2026-05-18-crabka-operator-kafkatopic-35-design.md).

**Tech stack:** Rust 2024, `kube-rs` (Controller + Finalizer), `k8s-openapi`, `schemars`, `serde_json`, `crabka-client-core` (typed connection), `crabka-protocol` (owned request types), Helm, kind, JVM `kafka-topics` / `kafka-configs` (e2e differential).

---

## Batch overview

| Batch | Tasks | Files (disjoint within batch) | Parallel? |
|---|---|---|---|
| 1 | T1, T2, T4 | `crates/client-admin/**` + workspace `Cargo.toml` ‖ `crates/operator/src/crd/topic.rs` + `crd/mod.rs` ‖ `charts/.../clusterrole.yaml` | yes |
| 2 | T3 | `crates/operator/src/controller/topic.rs` + `controller/mod.rs` + `context.rs` + `run.rs` + `gen_crds.rs` + `tests/reconcile_topic.rs` + `tests/shared/mod.rs` | — |
| 3 | T5, T6 | `deploy/crds/crabka.io_kafkatopics.yaml` (regen) ‖ `.github/workflows/operator-e2e.yml` | yes |

Dependencies: T3 imports T1's `AdminClient` and T2's `KafkaTopic`. T5 depends on T3 (uses `gen_crds.rs` which is modified in T3). T6 references T3's `KafkaTopic` status condition shape and T1's e2e plumbing indirectly.

---

## Task 1 — New crate `crates/client-admin`

**Files:**
- Create: `crates/client-admin/Cargo.toml`
- Create: `crates/client-admin/src/lib.rs`
- Create: `crates/client-admin/src/topics.rs`
- Create: `crates/client-admin/src/configs.rs`
- Create: `crates/client-admin/tests/round_trip.rs`
- Modify: `Cargo.toml` (workspace)

- [ ] **Step 1: Add the crate to workspace `Cargo.toml`**

In the top-level `Cargo.toml`, add `"crates/client-admin"` to `[workspace] members`, and add the workspace-dep entry:

```toml
[workspace.dependencies]
# … existing entries …
crabka-client-admin = { path = "crates/client-admin", version = "0.1.1" }
```

- [ ] **Step 2: Create `crates/client-admin/Cargo.toml`**

```toml
[package]
name = "crabka-client-admin"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Operator-side admin client for Crabka clusters"

[lints]
workspace = true

[dependencies]
crabka-client-core = { workspace = true }
crabka-protocol    = { workspace = true }
bytes              = { workspace = true }
thiserror          = { workspace = true }
tokio              = { workspace = true, features = ["sync", "time", "net"] }
tracing            = { workspace = true }
uuid               = { workspace = true }

[dev-dependencies]
crabka-broker = { workspace = true }
tempfile      = { workspace = true }
tokio         = { workspace = true, features = ["macros", "rt-multi-thread", "test-util"] }
tracing-subscriber = { workspace = true }
```

- [ ] **Step 3: Create `crates/client-admin/src/lib.rs` (entry + types + connect + retry helper)**

```rust
//! Slice 35: admin client for the operator. Targets one cluster's
//! controller; plaintext only (slice 36 will add TLS / SASL).
//!
//! Built on `crabka_client_core::Connection`'s typed
//! `send::<R: ProtocolRequest>` so request-version negotiation is
//! automatic via the `ApiVersionTable` populated at connect time.

use std::collections::BTreeMap;
use std::time::Duration;

use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use thiserror::Error;
use uuid::Uuid;

pub mod configs;
pub mod topics;

pub use configs::{
    AlterConfigsOutcome, IncrementalAlterOp, TopicConfigOverrides,
};
pub use topics::{
    CreatePartitionsOp, CreatePartitionsOutcome, CreateTopicOutcome,
    CreateTopicSpec, DeleteTopicOutcome, TopicMetadata, TopicMetadataEntry,
};

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("no bootstrap address was reachable: tried {tried}")]
    Connect { tried: usize },
    #[error("controller routing failed after retry")]
    NotControllerExhausted,
    #[error("broker returned error: api={api} code={code} ({name}){detail}",
            detail = .message.as_deref().map(|m| format!(" {m:?}")).unwrap_or_default())]
    Broker {
        api: &'static str,
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    #[error("client-core: {0}")]
    Transport(#[from] ClientError),
    #[error("protocol: {0}")]
    Protocol(String),
}

#[derive(Debug, Clone)]
pub struct KafkaError {
    pub code: i16,
    pub name: &'static str,
    pub message: Option<String>,
}

/// Short-lived admin client targeting one cluster's controller.
/// Plaintext only.
pub struct AdminClient {
    conn: Connection,
}

impl AdminClient {
    /// Try each bootstrap address in order. Each entry is `host:port`;
    /// DNS is resolved via `tokio::net::lookup_host`. First successful
    /// connect wins. Returns `AdminError::Connect { tried }` if none
    /// responded.
    pub async fn connect(bootstrap_addrs: &[String]) -> Result<Self, AdminError> {
        let opts = ConnectionOptions {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            client_id: "crabka-operator".to_string(),
        };
        for host_port in bootstrap_addrs {
            match Self::connect_one(host_port, opts.clone()).await {
                Ok(conn) => return Ok(Self { conn }),
                Err(e) => {
                    tracing::debug!(target: "crabka_client_admin", addr = %host_port, error = %e, "bootstrap connect failed");
                }
            }
        }
        Err(AdminError::Connect { tried: bootstrap_addrs.len() })
    }

    async fn connect_one(
        host_port: &str,
        opts: ConnectionOptions,
    ) -> Result<Connection, AdminError> {
        let mut addrs = tokio::net::lookup_host(host_port)
            .await
            .map_err(|e| AdminError::Protocol(format!("DNS lookup {host_port}: {e}")))?;
        let addr = addrs
            .next()
            .ok_or_else(|| AdminError::Protocol(format!("no addresses for {host_port}")))?;
        Connection::connect(addr, opts).await.map_err(AdminError::from)
    }

    /// Replace the underlying connection. Used internally by the
    /// `NOT_CONTROLLER` retry path to reconnect to the current controller.
    pub(crate) async fn reconnect(&mut self, host_port: &str) -> Result<(), AdminError> {
        let opts = ConnectionOptions {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            client_id: "crabka-operator".to_string(),
        };
        self.conn = Self::connect_one(host_port, opts).await?;
        Ok(())
    }
}

/// Kafka error code constants used by the retry path.
pub(crate) const NOT_CONTROLLER: i16 = 41;

/// Map a Kafka error code into a static name string for human-friendly
/// `AdminError::Broker` formatting. Only the codes we actually surface
/// today; unknown codes serialize as `"UNKNOWN"`.
pub(crate) fn kafka_error_name(code: i16) -> &'static str {
    match code {
        0 => "NONE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        7 => "REQUEST_TIMED_OUT",
        17 => "INVALID_TOPIC_EXCEPTION",
        19 => "NOT_ENOUGH_REPLICAS",
        36 => "TOPIC_ALREADY_EXISTS",
        37 => "INVALID_PARTITIONS",
        38 => "INVALID_REPLICATION_FACTOR",
        39 => "INVALID_REPLICA_ASSIGNMENT",
        40 => "INVALID_CONFIG",
        41 => "NOT_CONTROLLER",
        87 => "REASSIGNMENT_IN_PROGRESS",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_error_name_known_codes() {
        assert_eq!(kafka_error_name(0), "NONE");
        assert_eq!(kafka_error_name(36), "TOPIC_ALREADY_EXISTS");
        assert_eq!(kafka_error_name(41), "NOT_CONTROLLER");
    }

    #[test]
    fn kafka_error_name_unknown_returns_unknown() {
        assert_eq!(kafka_error_name(9999), "UNKNOWN");
    }
}
```

- [ ] **Step 4: Create `crates/client-admin/src/topics.rs`**

This module wraps `MetadataRequest`, `CreateTopicsRequest`, `DeleteTopicsRequest`, `CreatePartitionsRequest`. Pattern: build the owned request from the public input types, call `self.conn.send(req).await`, translate the response into the public outcome types. The implementer should follow the existing usage of these types in `crates/broker/tests/integration.rs` for shape reference.

```rust
//! Topic CRUD wrappers.

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    create_partitions_request::{
        CreatePartitionsRequest, CreatePartitionsTopic,
    },
    create_topics_request::{
        CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
        CreateTopicsRequest,
    },
    delete_topics_request::DeleteTopicsRequest,
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};
use uuid::Uuid;

use crate::{AdminClient, AdminError, KafkaError, NOT_CONTROLLER, kafka_error_name};

#[derive(Debug, Clone)]
pub struct CreateTopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicOutcome {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsOp {
    pub name: String,
    pub new_total_count: i32,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub controller_id: i32,
    pub topics: Vec<TopicMetadataEntry>,
}

#[derive(Debug, Clone)]
pub struct TopicMetadataEntry {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaError>,
}

impl AdminClient {
    /// Metadata for the named topics. Pass an empty slice to fetch all
    /// topics, per Kafka semantics.
    pub async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        let req = MetadataRequest {
            topics: if topics.is_empty() {
                None
            } else {
                Some(
                    topics
                        .iter()
                        .map(|n| MetadataRequestTopic {
                            topic_id: None,
                            name: Some((*n).to_string()),
                        })
                        .collect(),
                )
            },
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };
        let resp = self.conn.send(req).await?;
        Ok(parse_metadata(resp))
    }

    pub async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout_ms: i32,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        let first = {
            let req = build_create_topics(specs, timeout_ms);
            let resp = self.conn.send(req).await?;
            parse_create_topics(resp)
        };
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = {
            let req = build_create_topics(specs, timeout_ms);
            let resp = self.conn.send(req).await?;
            parse_create_topics(resp)
        };
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    pub async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        let build = || DeleteTopicsRequest {
            topic_names: names.iter().map(|s| (*s).to_string()).collect(),
            topics: vec![],
            timeout_ms,
        };
        let first = parse_delete_topics(self.conn.send(build()).await?);
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = parse_delete_topics(self.conn.send(build()).await?);
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    pub async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout_ms: i32,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        let build = || CreatePartitionsRequest {
            topics: ops
                .iter()
                .map(|o| CreatePartitionsTopic {
                    name: o.name.clone(),
                    count: o.new_total_count,
                    assignments: None,
                })
                .collect(),
            timeout_ms,
            validate_only: false,
        };
        let first = parse_create_partitions(self.conn.send(build()).await?);
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = parse_create_partitions(self.conn.send(build()).await?);
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    /// Fetch Metadata, find the controller's `host:port`, and replace
    /// `self.conn` with a connection to it. Used by the per-method
    /// `NOT_CONTROLLER` retry paths above.
    async fn refresh_controller_connection(&mut self) -> Result<(), AdminError> {
        let md_req = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };
        let md_resp = self.conn.send(md_req).await?;
        let controller_addr = controller_endpoint(&md_resp)
            .ok_or(AdminError::NotControllerExhausted)?;
        self.reconnect(&controller_addr).await
    }
}

fn any_not_controller<T, F: Fn(&T) -> Option<&KafkaError>>(items: &[T], get_err: F) -> bool {
    items.iter().any(|o| matches!(get_err(o), Some(e) if e.code == NOT_CONTROLLER))
}

fn build_create_topics(specs: &[CreateTopicSpec], timeout_ms: i32) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: specs
            .iter()
            .map(|s| CreatableTopic {
                name: s.name.clone(),
                num_partitions: s.partitions,
                replication_factor: i16::try_from(s.replicas).unwrap_or(i16::MAX),
                assignments: vec![],
                configs: s
                    .configs
                    .iter()
                    .map(|(k, v)| CreatableTopicConfig {
                        name: k.clone(),
                        value: Some(v.clone()),
                    })
                    .collect(),
            })
            .collect(),
        timeout_ms,
        validate_only: false,
    }
}

fn parse_create_topics(
    resp: <CreateTopicsRequest as crabka_protocol::codec::ProtocolRequest>::Response,
) -> Vec<CreateTopicOutcome> {
    resp.topics
        .into_iter()
        .map(|t| CreateTopicOutcome {
            name: t.name,
            topic_id: t.topic_id.filter(|u| !u.is_nil()),
            error: error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_delete_topics(
    resp: <DeleteTopicsRequest as crabka_protocol::codec::ProtocolRequest>::Response,
) -> Vec<DeleteTopicOutcome> {
    resp.responses
        .into_iter()
        .map(|t| DeleteTopicOutcome {
            name: t.name.unwrap_or_default(),
            error: error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_create_partitions(
    resp: <CreatePartitionsRequest as crabka_protocol::codec::ProtocolRequest>::Response,
) -> Vec<CreatePartitionsOutcome> {
    resp.results
        .into_iter()
        .map(|t| CreatePartitionsOutcome {
            name: t.name,
            error: error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_metadata(
    resp: <MetadataRequest as crabka_protocol::codec::ProtocolRequest>::Response,
) -> TopicMetadata {
    let topics = resp
        .topics
        .into_iter()
        .map(|t| {
            let partition_count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
            let replication_factor = i32::from(
                t.partitions
                    .first()
                    .map(|p| i16::try_from(p.replica_nodes.len()).unwrap_or(i16::MAX))
                    .unwrap_or(0),
            );
            TopicMetadataEntry {
                name: t.name.unwrap_or_default(),
                topic_id: t.topic_id.filter(|u| !u.is_nil()),
                partition_count,
                replication_factor,
                error: error_if(t.error_code, None),
            }
        })
        .collect();
    TopicMetadata {
        controller_id: resp.controller_id,
        topics,
    }
}

fn controller_endpoint(
    resp: &<MetadataRequest as crabka_protocol::codec::ProtocolRequest>::Response,
) -> Option<String> {
    let id = resp.controller_id;
    resp.brokers
        .iter()
        .find(|b| b.node_id == id)
        .map(|b| format!("{}:{}", b.host, b.port))
}

fn error_if(code: i16, message: Option<String>) -> Option<KafkaError> {
    if code == 0 {
        None
    } else {
        Some(KafkaError {
            code,
            name: kafka_error_name(code),
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn build_create_topics_one_spec() {
        let req = build_create_topics(
            &[CreateTopicSpec {
                name: "foo".into(),
                partitions: 3,
                replicas: 1,
                configs: BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]),
            }],
            5_000,
        );
        assert_eq!(req.topics.len(), 1);
        let t = &req.topics[0];
        assert_eq!(t.name, "foo");
        assert_eq!(t.num_partitions, 3);
        assert_eq!(t.replication_factor, 1);
        assert_eq!(t.configs.len(), 1);
        assert_eq!(t.configs[0].name, "retention.ms");
        assert_eq!(t.configs[0].value.as_deref(), Some("60000"));
        assert_eq!(req.timeout_ms, 5_000);
        assert!(!req.validate_only);
    }

    #[test]
    fn error_if_zero_code_is_none() {
        assert!(error_if(0, None).is_none());
    }

    #[test]
    fn error_if_nonzero_carries_name() {
        let e = error_if(36, Some("dup".into())).unwrap();
        assert_eq!(e.code, 36);
        assert_eq!(e.name, "TOPIC_ALREADY_EXISTS");
        assert_eq!(e.message.as_deref(), Some("dup"));
    }
}
```

> The implementer must verify the exact field names on the owned request types (`CreatableTopic.num_partitions` vs `partitions`, `DeleteTopicsResponse.responses` vs `topics`, etc.) against `crates/protocol/src/owned/*.rs`. If a field name diverges, prefer the protocol crate's name. The build will tell you immediately.

- [ ] **Step 5: Create `crates/client-admin/src/configs.rs`**

```rust
//! Topic-config wrappers.
//!
//! `DescribeConfigs` filters to the subset of entries the user/operator
//! has explicitly set (i.e. dynamic topic config, ConfigSource =
//! `DYNAMIC_TOPIC_CONFIG = 1`), so the diff against `spec.config` is
//! against overrides only — never broker defaults.

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    describe_configs_request::{
        DescribeConfigsRequest, DescribeConfigsResource,
    },
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
};

use crate::{AdminClient, AdminError, KafkaError, kafka_error_name};

/// `ConfigSource = DYNAMIC_TOPIC_CONFIG` per
/// https://kafka.apache.org/protocol#The_Messages_DescribeConfigs.
const DYNAMIC_TOPIC_CONFIG_SOURCE: i8 = 1;

/// Kafka's `ConfigResource.type` for topic resources.
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Per-topic dynamic config overrides (broker defaults are filtered out).
#[derive(Debug, Clone, Default)]
pub struct TopicConfigOverrides {
    pub topic: String,
    pub overrides: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum IncrementalAlterOp {
    Set { topic: String, key: String, value: String },
    Delete { topic: String, key: String },
}

#[derive(Debug, Clone)]
pub struct AlterConfigsOutcome {
    pub topic: String,
    pub error: Option<KafkaError>,
}

impl AdminClient {
    pub async fn describe_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<TopicConfigOverrides>, AdminError> {
        let req = DescribeConfigsRequest {
            resources: topics
                .iter()
                .map(|t| DescribeConfigsResource {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: (*t).to_string(),
                    configuration_keys: None,
                })
                .collect(),
            include_synonyms: false,
            include_documentation: false,
        };
        let resp = self.conn.send(req).await?;
        let mut out = Vec::with_capacity(resp.results.len());
        for r in resp.results {
            if r.error_code != 0 {
                return Err(AdminError::Broker {
                    api: "DescribeConfigs",
                    code: r.error_code,
                    name: kafka_error_name(r.error_code),
                    message: r.error_message,
                });
            }
            let mut overrides = BTreeMap::new();
            for entry in r.configs {
                if entry.config_source == DYNAMIC_TOPIC_CONFIG_SOURCE {
                    if let (name, Some(value)) = (entry.name, entry.value) {
                        overrides.insert(name, value);
                    }
                }
            }
            out.push(TopicConfigOverrides {
                topic: r.resource_name,
                overrides,
            });
        }
        Ok(out)
    }

    pub async fn incremental_alter_configs(
        &mut self,
        ops: &[IncrementalAlterOp],
    ) -> Result<Vec<AlterConfigsOutcome>, AdminError> {
        // Group ops by topic.
        let mut by_topic: BTreeMap<String, Vec<AlterableConfig>> = BTreeMap::new();
        for op in ops {
            match op {
                IncrementalAlterOp::Set { topic, key, value } => {
                    by_topic.entry(topic.clone()).or_default().push(AlterableConfig {
                        name: key.clone(),
                        config_operation: 0,  // SET
                        value: Some(value.clone()),
                    });
                }
                IncrementalAlterOp::Delete { topic, key } => {
                    by_topic.entry(topic.clone()).or_default().push(AlterableConfig {
                        name: key.clone(),
                        config_operation: 1,  // DELETE
                        value: None,
                    });
                }
            }
        }
        let req = IncrementalAlterConfigsRequest {
            resources: by_topic
                .into_iter()
                .map(|(topic, configs)| AlterConfigsResource {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: topic,
                    configs,
                })
                .collect(),
            validate_only: false,
        };
        let resp = self.conn.send(req).await?;
        Ok(resp
            .responses
            .into_iter()
            .map(|r| AlterConfigsOutcome {
                topic: r.resource_name,
                error: if r.error_code == 0 {
                    None
                } else {
                    Some(KafkaError {
                        code: r.error_code,
                        name: kafka_error_name(r.error_code),
                        message: r.error_message,
                    })
                },
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_topic_config_source_is_one() {
        // Guard so a future protocol change can't silently flip the
        // filter we use to distinguish overrides from broker defaults.
        assert_eq!(DYNAMIC_TOPIC_CONFIG_SOURCE, 1);
    }

    #[test]
    fn resource_type_topic_is_two() {
        assert_eq!(RESOURCE_TYPE_TOPIC, 2);
    }
}
```

> Verify field names in `crates/protocol/src/owned/describe_configs_response.rs` and `incremental_alter_configs_*.rs`. The op-codes `0=SET, 1=DELETE` are Kafka protocol constants.

- [ ] **Step 6: Create `crates/client-admin/tests/round_trip.rs`**

```rust
//! Integration test: spin up an in-process broker via the existing
//! `crates/broker/tests/support` harness, drive every admin RPC slice
//! 35 needs through `AdminClient`, assert the visible cluster state.

#![cfg(not(target_os = "windows"))]

use std::collections::BTreeMap;
use std::time::Duration;

use crabka_client_admin::{
    AdminClient, CreatePartitionsOp, CreateTopicSpec, IncrementalAlterOp,
};

#[path = "../../broker/tests/support/mod.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_round_trip_create_alter_delete() {
    support::init_tracing();
    let proc = support::start().await;
    let bootstrap = proc.broker.listen_addr().to_string();

    let mut admin = AdminClient::connect(&[bootstrap]).await.unwrap();

    // 1. Topic doesn't exist initially.
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert!(foo.error.is_some(), "expected error for unknown topic");

    // 2. Create the topic with one config override.
    let configs = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
    let outcomes = admin
        .create_topics(&[CreateTopicSpec {
            name: "foo".into(),
            partitions: 3,
            replicas: 1,
            configs,
        }], 5_000)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(outcomes[0].error.is_none(), "create failed: {:?}", outcomes[0].error);

    // 3. Metadata reflects the create.
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert!(foo.error.is_none());
    assert_eq!(foo.partition_count, 3);
    assert_eq!(foo.replication_factor, 1);

    // 4. Increase partitions to 5.
    let outcomes = admin
        .create_partitions(&[CreatePartitionsOp {
            name: "foo".into(),
            new_total_count: 5,
        }], 5_000)
        .await
        .unwrap();
    assert!(outcomes[0].error.is_none());
    // Brief wait for metadata refresh.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert_eq!(foo.partition_count, 5);

    // 5. describe_configs reports retention.ms as a dynamic override.
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert_eq!(overrides.len(), 1);
    assert_eq!(
        overrides[0].overrides.get("retention.ms").map(String::as_str),
        Some("60000"),
    );

    // 6. incremental_alter SET a new key.
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Set {
            topic: "foo".into(),
            key: "cleanup.policy".into(),
            value: "compact".into(),
        }])
        .await
        .unwrap();
    assert!(outcomes[0].error.is_none());
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert_eq!(
        overrides[0].overrides.get("cleanup.policy").map(String::as_str),
        Some("compact"),
    );

    // 7. incremental_alter DELETE the retention override.
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Delete {
            topic: "foo".into(),
            key: "retention.ms".into(),
        }])
        .await
        .unwrap();
    assert!(outcomes[0].error.is_none());
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert!(!overrides[0].overrides.contains_key("retention.ms"));

    // 8. Delete the topic.
    let outcomes = admin.delete_topics(&["foo"], 5_000).await.unwrap();
    assert!(outcomes[0].error.is_none());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo");
    if let Some(t) = foo {
        assert!(t.error.is_some(), "deleted topic should report unknown");
    }
}
```

- [ ] **Step 7: Run the unit tests**

Run: `cargo test -p crabka-client-admin --lib`
Expected: ~5 unit tests pass.

- [ ] **Step 8: Run the integration test**

Run: `cargo test -p crabka-client-admin --test round_trip`
Expected: PASS. Takes 5-15s (in-process broker startup).

- [ ] **Step 9: Clippy clean**

Run: `cargo clippy -p crabka-client-admin --all-targets -- -D warnings`
Expected: clean. If a `clippy::doc_markdown` warning fires on `NotControllerExhausted` or similar in doc comments, add backticks.

- [ ] **Step 10: Commit**

```bash
git add crates/client-admin Cargo.toml
git commit -m "Slice 35 T1: crates/client-admin — AdminClient for the operator"
```

---

## Task 2 — CRD types: `KafkaTopic`

**Files:**
- Create: `crates/operator/src/crd/topic.rs`
- Modify: `crates/operator/src/crd/mod.rs`

- [ ] **Step 1: Create `crates/operator/src/crd/topic.rs`**

```rust
//! Slice 35: `KafkaTopic` CRD. Strimzi-shaped; unidirectional
//! reconciliation (CRD wins).

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaTopic",
    plural = "kafkatopics",
    singular = "kafkatopic",
    shortname = "kt",
    namespaced,
    status = "KafkaTopicStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicSpec {
    /// Optional override for the Kafka topic name. Defaults to
    /// `metadata.name`. Validated at reconcile time against Kafka's
    /// rules (length ≤ 249, chars `[A-Za-z0-9._-]`, not `.` or `..`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Number of partitions. Increases honored via CreatePartitions;
    /// decreases rejected with `ImmutableFieldChanged`.
    #[schemars(range(min = 1, max = 1_000_000))]
    pub partitions: i32,

    /// Replication factor. Changes rejected with
    /// `ImmutableFieldChanged` until partition reassignment lands
    /// (slice 43+).
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Opaque topic-level config (`retention.ms`, `cleanup.policy`,
    /// etc.). Reconciled via IncrementalAlterConfigs SET/DELETE diff
    /// against the cluster's current dynamic-topic overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<BTreeMap<String, String>>,

    /// When `true`, CRD delete still removes the finalizer but skips
    /// the `DeleteTopics` call so the Kafka topic survives. Default
    /// `false`.
    #[serde(default)]
    pub preserve_topic: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStatus {
    /// Standard Kubernetes-style condition list. Surfaces `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec
    /// (i.e. last time we wrote `Ready=True reason=Ready`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Effective topic name (defaulted if `spec.topicName` unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Cluster-assigned topic UUID, populated once the topic exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt as _;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaTopic::crd();
        assert_eq!(crd.spec.group, "crabka.io");
        assert_eq!(crd.spec.names.kind, "KafkaTopic");
        assert_eq!(crd.spec.names.plural, "kafkatopics");
        assert!(
            crd.spec.names.short_names.as_ref()
                .is_some_and(|v| v.contains(&"kt".to_string())),
            "expected shortname `kt`",
        );
        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn spec_round_trips_through_json() {
        let kt = KafkaTopic::new(
            "demo-topic",
            KafkaTopicSpec {
                topic_name: Some("Demo.Topic".into()),
                partitions: 3,
                replicas: 2,
                config: Some(BTreeMap::from([
                    ("retention.ms".to_string(), "60000".to_string()),
                ])),
                preserve_topic: true,
            },
        );
        let json = serde_json::to_string(&kt).unwrap();
        assert!(json.contains("\"topicName\":\"Demo.Topic\""), "got: {json}");
        assert!(json.contains("\"partitions\":3"), "got: {json}");
        assert!(json.contains("\"preserveTopic\":true"), "got: {json}");
        let back: KafkaTopic = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, kt.spec);
    }

    #[test]
    fn spec_omits_optional_fields_when_default() {
        let kt = KafkaTopic::new(
            "demo",
            KafkaTopicSpec {
                topic_name: None,
                partitions: 1,
                replicas: 1,
                config: None,
                preserve_topic: false,
            },
        );
        let j = serde_json::to_string(&kt.spec).unwrap();
        assert!(!j.contains("topicName"), "got: {j}");
        assert!(!j.contains("config"), "got: {j}");
        // `preserveTopic` is a plain bool — serde emits it.
        assert!(j.contains("\"preserveTopic\":false"), "got: {j}");
    }

    #[test]
    fn status_topic_id_omitted_when_none() {
        let status = KafkaTopicStatus {
            conditions: vec![],
            observed_generation: Some(1),
            topic_name: Some("foo".into()),
            topic_id: None,
        };
        let j = serde_json::to_string(&status).unwrap();
        assert!(!j.contains("topicId"), "got: {j}");
        assert!(j.contains("\"observedGeneration\":1"), "got: {j}");
    }

    #[test]
    fn minimum_required_spec_parses() {
        let json = r#"{"partitions":1,"replicas":1}"#;
        let spec: KafkaTopicSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.partitions, 1);
        assert_eq!(spec.replicas, 1);
        assert!(spec.topic_name.is_none());
        assert!(spec.config.is_none());
        assert!(!spec.preserve_topic);
    }
}
```

- [ ] **Step 2: Update `crates/operator/src/crd/mod.rs`**

Add `pub mod topic;` and re-export. The full file becomes:

```rust
//! CRD type definitions. Each kind lives in its own submodule and is the
//! single source of truth for both the runtime types and the generated
//! CRD YAML manifest (see `gen_crds`).

pub mod kafka;
pub mod kafka_node_pool;
pub mod listener;
pub mod metrics;
pub mod network_policy;
pub mod topic;

pub use kafka::{Kafka, KafkaCondition, KafkaSpec, KafkaStatus};
pub use kafka_node_pool::{
    KafkaNodePool, KafkaNodePoolSpec, KafkaNodePoolStatus, MetadataTemplate, NodeRole,
    PersistentClaimSpec, PodTemplate, Storage,
};
pub use listener::*;
pub use metrics::{MetricsConfig, MetricsType, PodMonitorSpec, ServiceMonitorSpec};
pub use network_policy::{NetworkPolicyPeer, NetworkPolicySpec};
pub use topic::{KafkaTopic, KafkaTopicSpec, KafkaTopicStatus};
```

- [ ] **Step 3: Run the CRD tests**

Run: `cargo test -p crabka-operator --lib crd::topic`
Expected: 5 tests pass.

- [ ] **Step 4: Clippy clean**

Run: `cargo clippy -p crabka-operator --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/operator/src/crd/topic.rs crates/operator/src/crd/mod.rs
git commit -m "Slice 35 T2: KafkaTopic CRD types"
```

---

## Task 3 — Controller wiring, context, reconcile tests

**Depends on:** T1 (uses `crabka_client_admin::AdminClient`), T2 (uses `crabka_operator::crd::KafkaTopic`).

**Files:**
- Create: `crates/operator/src/controller/topic.rs`
- Modify: `crates/operator/src/controller/mod.rs`
- Modify: `crates/operator/src/context.rs`
- Modify: `crates/operator/src/run.rs`
- Modify: `crates/operator/src/gen_crds.rs`
- Modify: `crates/operator/Cargo.toml` (add `crabka-client-admin` dep)
- Create: `crates/operator/tests/reconcile_topic.rs`

- [ ] **Step 1: Add `crabka-client-admin` to `crates/operator/Cargo.toml`**

In `[dependencies]`, alongside `kube` and `k8s-openapi`:

```toml
crabka-client-admin = { workspace = true }
```

- [ ] **Step 2: Extend `Context` with an admin-client cache**

`crates/operator/src/context.rs` becomes:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use crabka_client_admin::AdminClient;
use kube::Client;
use tokio::sync::Mutex;

use crate::config::OperatorConfig;
use crate::telemetry::SharedRegistry;

/// Shared per-reconciler context. Cheap to clone (all fields Arc /
/// shared via interior mutability).
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
    /// Per-cluster admin-client cache. Keyed by `Kafka` resource name.
    /// Broken connections are replaced lazily on next use.
    pub admin_clients: Arc<Mutex<HashMap<String, Arc<Mutex<AdminClient>>>>>,
}

impl Context {
    #[must_use]
    pub fn new(client: Client, config: OperatorConfig, registry: SharedRegistry) -> Self {
        Self {
            client,
            config: Arc::new(config),
            registry,
            admin_clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Look up or open an `AdminClient` for the named cluster.
    ///
    /// `bootstrap` is the inter-broker listener's `bootstrap_servers`
    /// string, e.g. `demo-broker-headless.default.svc.cluster.local:9092`.
    pub async fn admin_client_for(
        &self,
        cluster: &str,
        bootstrap: &str,
    ) -> Result<Arc<Mutex<AdminClient>>, crabka_client_admin::AdminError> {
        let mut map = self.admin_clients.lock().await;
        if let Some(client) = map.get(cluster) {
            return Ok(client.clone());
        }
        let admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
        let entry = Arc::new(Mutex::new(admin));
        map.insert(cluster.to_string(), entry.clone());
        Ok(entry)
    }

    /// Drop the cached admin client for `cluster` (used by reconcile when
    /// a Transport error indicates the connection died — next call will
    /// reopen).
    pub async fn drop_admin_client(&self, cluster: &str) {
        self.admin_clients.lock().await.remove(cluster);
    }
}
```

- [ ] **Step 3: Create `crates/operator/src/controller/topic.rs`**

This is the largest file in the slice. The implementer should write it as documented in the spec §4. Skeleton + full diff helpers + tests below.

```rust
//! Slice 35: `KafkaTopic` reconciler — unidirectional (CRD wins).
//!
//! Watches `KafkaTopic` (primary) and `Kafka` (secondary, so a cluster
//! becoming Ready wakes pending topic reconciles). Diff-and-apply
//! against the live cluster via `crabka_client_admin::AdminClient`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crabka_client_admin::{
    AdminClient, AdminError, CreatePartitionsOp, CreateTopicSpec,
    IncrementalAlterOp,
};
use futures::StreamExt as _;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};
use serde_json::json;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::controller::common::{FIELD_MANAGER, ReconcileError, condition};
use crate::crd::{Kafka, KafkaCondition, KafkaTopic, KafkaTopicStatus};

const FINALIZER: &str = "crabka.io/topic-finalizer";

/// Run the controller forever.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let topic_api: Api<KafkaTopic> = Api::all(ctx.client.clone());
    let kafka_api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(topic_api, watcher::Config::default())
        // Kafka watch wakes the reconcile loop on cluster status changes.
        // We return empty here (rather than listing matching topics) so
        // the mapper stays sync-safe — listing would require an async
        // call that the kube-rs `mapper` signature doesn't allow. The
        // 60-second periodic requeue on each KafkaTopic catches the
        // transition (matches how `kafka.rs` handles its Node watch).
        .watches(kafka_api, watcher::Config::default(), |_kafka| {
            Vec::<ObjectRef<KafkaTopic>>::new().into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "topic reconciled"),
                Err(e) => tracing::warn!(error = %e, "topic reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<KafkaTopic>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "topic reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

#[allow(clippy::too_many_lines)] // linear pipeline; extraction hurts more than helps
pub async fn reconcile(obj: Arc<KafkaTopic>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let topic_api: Api<KafkaTopic> = Api::namespaced(ctx.client.clone(), &ns);

    // 1. Cluster label
    let cluster = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned());
    let Some(cluster) = cluster else {
        patch_status(
            &topic_api, &name, &obj,
            "False", "MissingClusterLabel",
            "metadata.labels[\"crabka.io/cluster\"] is required",
            None, false,
        ).await?;
        return Ok(Action::requeue(Duration::from_secs(60)));
    };

    // 2. Effective topic name
    let topic_name = obj.spec.topic_name.clone().unwrap_or_else(|| name.clone());
    if let Err(msg) = validate_kafka_topic_name(&topic_name) {
        patch_status(
            &topic_api, &name, &obj,
            "False", "InvalidTopicName", &msg, None, false,
        ).await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // 3. Look up the Kafka + bootstrap
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let kafka = kafka_api.get_opt(&cluster).await?;
    let bootstrap = kafka.as_ref().and_then(internal_listener_bootstrap);
    let Some(bootstrap) = bootstrap else {
        patch_status(
            &topic_api, &name, &obj,
            "False", "ClusterNotReady",
            &format!("Kafka/{cluster} not Ready or no internal listener"),
            None, false,
        ).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    // 4. Finalizer / delete path
    if obj.meta().deletion_timestamp.is_some() {
        if !obj.spec.preserve_topic {
            // Best-effort: log non-UNKNOWN_TOPIC errors but don't propagate
            // (we want the finalizer removal to succeed even if the cluster
            // is gone).
            let client = ctx.admin_client_for(&cluster, &bootstrap).await;
            if let Ok(client) = client {
                let mut admin = client.lock().await;
                match admin.delete_topics(&[&topic_name], 30_000).await {
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, %topic_name, "DeleteTopics failed during finalizer"),
                }
            }
        }
        remove_finalizer(&topic_api, &name).await?;
        return Ok(Action::await_change());
    }

    // 5. Ensure finalizer
    if !has_finalizer(&obj) {
        add_finalizer(&topic_api, &name).await?;
        return Ok(Action::requeue(Duration::ZERO));  // re-enter
    }

    // 6. Connect and fetch current state
    let admin_handle = match ctx.admin_client_for(&cluster, &bootstrap).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, %cluster, "AdminClient connect failed");
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let mut admin = admin_handle.lock().await;

    let md = match admin.metadata(&[&topic_name]).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, %topic_name, "Metadata failed");
            ctx.drop_admin_client(&cluster).await;
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let current = md.topics.iter().find(|t| t.name == topic_name);

    // 7. Diff and apply
    let current = match current {
        Some(t) if t.error.is_none() => Some(t.clone()),
        _ => None,
    };
    match current {
        None => {
            // CreateTopics
            let outcome_vec = admin
                .create_topics(
                    &[CreateTopicSpec {
                        name: topic_name.clone(),
                        partitions: obj.spec.partitions,
                        replicas: obj.spec.replicas,
                        configs: obj.spec.config.clone().unwrap_or_default(),
                    }],
                    30_000,
                )
                .await;
            let outcome = match outcome_vec {
                Ok(mut v) => v.pop().expect("one spec → one outcome"),
                Err(e) => {
                    tracing::warn!(error = %e, "CreateTopics transport failure");
                    return Ok(Action::requeue(Duration::from_secs(15)));
                }
            };
            if let Some(err) = outcome.error {
                patch_status(
                    &topic_api, &name, &obj,
                    "False", "BrokerError",
                    &format!("CreateTopics: {} ({})", err.name, err.code),
                    None, false,
                ).await?;
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
            patch_status(
                &topic_api, &name, &obj,
                "True", "Ready", "topic created",
                outcome.topic_id.map(|u| u.to_string()),
                true,
            ).await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }
        Some(cur) => {
            // Immutable fields
            if cur.replication_factor != obj.spec.replicas {
                patch_status(
                    &topic_api, &name, &obj,
                    "False", "ImmutableFieldChanged",
                    "spec.replicas change requires partition reassignment (slice 43+)",
                    cur.topic_id.map(|u| u.to_string()),
                    false,
                ).await?;
                return Ok(Action::requeue(Duration::from_secs(300)));
            }
            if cur.partition_count > obj.spec.partitions {
                patch_status(
                    &topic_api, &name, &obj,
                    "False", "ImmutableFieldChanged",
                    "spec.partitions decrease is not supported by Kafka",
                    cur.topic_id.map(|u| u.to_string()),
                    false,
                ).await?;
                return Ok(Action::requeue(Duration::from_secs(300)));
            }

            // Partition increase
            if cur.partition_count < obj.spec.partitions {
                let outcomes = admin
                    .create_partitions(
                        &[CreatePartitionsOp {
                            name: topic_name.clone(),
                            new_total_count: obj.spec.partitions,
                        }],
                        30_000,
                    )
                    .await;
                match outcomes {
                    Ok(mut v) => {
                        let o = v.pop().expect("one op → one outcome");
                        if let Some(err) = o.error {
                            patch_status(
                                &topic_api, &name, &obj,
                                "False", "BrokerError",
                                &format!("CreatePartitions: {} ({})", err.name, err.code),
                                cur.topic_id.map(|u| u.to_string()),
                                false,
                            ).await?;
                            return Ok(Action::requeue(Duration::from_secs(15)));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "CreatePartitions transport failure");
                        return Ok(Action::requeue(Duration::from_secs(15)));
                    }
                }
            }

            // Config diff
            let desired = obj.spec.config.clone().unwrap_or_default();
            let overrides = match admin.describe_configs(&[&topic_name]).await {
                Ok(v) => v.into_iter().next().map(|o| o.overrides).unwrap_or_default(),
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeConfigs failed");
                    return Ok(Action::requeue(Duration::from_secs(15)));
                }
            };
            let ops = diff_configs(&overrides, &desired, &topic_name);
            if !ops.is_empty() {
                match admin.incremental_alter_configs(&ops).await {
                    Ok(outcomes) => {
                        if let Some(err) = outcomes.into_iter().find_map(|o| o.error) {
                            patch_status(
                                &topic_api, &name, &obj,
                                "False", "BrokerError",
                                &format!("IncrementalAlterConfigs: {} ({})", err.name, err.code),
                                cur.topic_id.map(|u| u.to_string()),
                                false,
                            ).await?;
                            return Ok(Action::requeue(Duration::from_secs(15)));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "IncrementalAlterConfigs failure");
                        return Ok(Action::requeue(Duration::from_secs(15)));
                    }
                }
            }

            patch_status(
                &topic_api, &name, &obj,
                "True", "Ready", "topic in sync",
                cur.topic_id.map(|u| u.to_string()),
                true,
            ).await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }
    }
}

/// Diff `desired` against `current` overrides; produce a Vec of
/// IncrementalAlterOps. Pure function — covered by tests below.
pub(crate) fn diff_configs(
    current: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
    topic: &str,
) -> Vec<IncrementalAlterOp> {
    let mut ops = Vec::new();
    for (k, v) in desired {
        if current.get(k) != Some(v) {
            ops.push(IncrementalAlterOp::Set {
                topic: topic.to_string(),
                key: k.clone(),
                value: v.clone(),
            });
        }
    }
    for k in current.keys() {
        if !desired.contains_key(k) {
            ops.push(IncrementalAlterOp::Delete {
                topic: topic.to_string(),
                key: k.clone(),
            });
        }
    }
    ops
}

/// Bootstrap address from `Kafka.status.listeners[<inter_broker>]`.
/// Returns `None` if `Kafka.status.conditions[Ready].status != "True"`.
pub(crate) fn internal_listener_bootstrap(kafka: &Kafka) -> Option<String> {
    let ready_true = kafka
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"))
        .is_some_and(|c| c.status == "True");
    if !ready_true {
        return None;
    }
    let inter_broker = kafka.spec.inter_broker_listener_name.as_deref().unwrap_or("PLAIN");
    let listeners = &kafka.status.as_ref()?.listeners;
    listeners
        .iter()
        .find(|l| l.name == inter_broker)
        .map(|l| l.bootstrap_servers.clone())
        .filter(|s| !s.is_empty())
}

/// Kafka topic name validation. Mirrors the JVM client's `Topic.validate`.
pub(crate) fn validate_kafka_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("topic name is empty".into());
    }
    if name.len() > 249 {
        return Err(format!("topic name length {} exceeds 249", name.len()));
    }
    if name == "." || name == ".." {
        return Err("topic name cannot be \".\" or \"..\"".into());
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-') {
        return Err(format!("topic name {name:?} contains invalid characters"));
    }
    Ok(())
}

fn has_finalizer(obj: &KafkaTopic) -> bool {
    obj.meta()
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|s| s == FINALIZER))
}

async fn add_finalizer(api: &Api<KafkaTopic>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [FINALIZER] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

async fn remove_finalizer(api: &Api<KafkaTopic>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

/// Build + patch status. `advance_generation = true` writes
/// `observedGeneration` to the current generation (only on successful
/// True/Ready landings).
#[allow(clippy::too_many_arguments)] // pure status helper; arity reflects the condition contract
async fn patch_status(
    api: &Api<KafkaTopic>,
    name: &str,
    obj: &KafkaTopic,
    status: &str,
    reason: &str,
    message: &str,
    topic_id: Option<String>,
    advance_generation: bool,
) -> Result<(), ReconcileError> {
    let topic_name = obj.spec.topic_name.clone().unwrap_or_else(|| name.to_string());
    let conditions = vec![condition("Ready", status, reason, message)];
    let observed_generation = if advance_generation {
        obj.meta().generation
    } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };

    let body = json!({
        "status": {
            "conditions": conditions,
            "observedGeneration": observed_generation,
            "topicName": topic_name,
            "topicId": topic_id,
        }
    });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch_status(name, &params, &Patch::Merge(&body)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{KafkaSpec, KafkaStatus, ListenerStatus, ListenerType};

    fn kafka_ready(name: &str, namespace: &str, listener_port: i32) -> Kafka {
        let mut k = Kafka::new(
            name,
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                config: None,
                listeners: vec![],
                inter_broker_listener_name: Some("PLAIN".into()),
                metrics_config: None,
                network_policy: None,
            },
        );
        k.metadata.namespace = Some(namespace.into());
        k.status = Some(KafkaStatus {
            conditions: vec![KafkaCondition {
                type_: "Ready".into(),
                status: "True".into(),
                reason: "Available".into(),
                message: "".into(),
                last_transition_time: "2026-05-18T00:00:00Z".into(),
            }],
            replicas: Some(1),
            ready_replicas: Some(1),
            listeners: vec![ListenerStatus {
                name: "PLAIN".into(),
                type_: ListenerType::Internal,
                bootstrap_servers: format!("{name}-broker-headless.{namespace}.svc.cluster.local:{listener_port}"),
                addresses: vec![],
            }],
        });
        k
    }

    #[test]
    fn validate_topic_name_accepts_typical() {
        assert!(validate_kafka_topic_name("demo-topic").is_ok());
        assert!(validate_kafka_topic_name("My.Topic_1").is_ok());
    }

    #[test]
    fn validate_topic_name_rejects_empty() {
        assert!(validate_kafka_topic_name("").is_err());
    }

    #[test]
    fn validate_topic_name_rejects_dot_and_dotdot() {
        assert!(validate_kafka_topic_name(".").is_err());
        assert!(validate_kafka_topic_name("..").is_err());
    }

    #[test]
    fn validate_topic_name_rejects_too_long() {
        let n = "a".repeat(250);
        assert!(validate_kafka_topic_name(&n).is_err());
    }

    #[test]
    fn validate_topic_name_rejects_invalid_chars() {
        assert!(validate_kafka_topic_name("has space").is_err());
        assert!(validate_kafka_topic_name("has/slash").is_err());
        assert!(validate_kafka_topic_name("has@at").is_err());
    }

    #[test]
    fn diff_configs_set_adds_missing_key() {
        let current = BTreeMap::new();
        let desired = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
        let ops = diff_configs(&current, &desired, "foo");
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], IncrementalAlterOp::Set { key, value, .. }
            if key == "retention.ms" && value == "60000"));
    }

    #[test]
    fn diff_configs_set_updates_changed_value() {
        let current = BTreeMap::from([("retention.ms".to_string(), "30000".to_string())]);
        let desired = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
        let ops = diff_configs(&current, &desired, "foo");
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], IncrementalAlterOp::Set { value, .. } if value == "60000"));
    }

    #[test]
    fn diff_configs_delete_removes_extra_key() {
        let current = BTreeMap::from([("cleanup.policy".to_string(), "delete".to_string())]);
        let desired = BTreeMap::new();
        let ops = diff_configs(&current, &desired, "foo");
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], IncrementalAlterOp::Delete { key, .. } if key == "cleanup.policy"));
    }

    #[test]
    fn diff_configs_noop_when_matching() {
        let m = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
        assert!(diff_configs(&m, &m, "foo").is_empty());
    }

    #[test]
    fn diff_configs_combines_set_and_delete() {
        let current = BTreeMap::from([
            ("retention.ms".to_string(), "30000".to_string()),
            ("cleanup.policy".to_string(), "delete".to_string()),
        ]);
        let desired = BTreeMap::from([
            ("retention.ms".to_string(), "60000".to_string()),
            ("segment.bytes".to_string(), "1048576".to_string()),
        ]);
        let ops = diff_configs(&current, &desired, "foo");
        assert_eq!(ops.len(), 3, "expected SET(retention.ms), SET(segment.bytes), DELETE(cleanup.policy)");
    }

    #[test]
    fn internal_listener_bootstrap_returns_listener_when_ready() {
        let k = kafka_ready("demo", "default", 9092);
        assert_eq!(
            internal_listener_bootstrap(&k).as_deref(),
            Some("demo-broker-headless.default.svc.cluster.local:9092"),
        );
    }

    #[test]
    fn internal_listener_bootstrap_returns_none_when_not_ready() {
        let mut k = kafka_ready("demo", "default", 9092);
        if let Some(s) = k.status.as_mut() {
            s.conditions[0].status = "False".into();
        }
        assert!(internal_listener_bootstrap(&k).is_none());
    }
}
```

- [ ] **Step 4: Declare the module in `controller/mod.rs`**

Append `pub mod topic;` (it needs `pub` so `run.rs` can call `controller::topic::run`):

```rust
//! Controllers (reconcilers) for Crabka CRDs. Each kind lives in its own
//! submodule and shares helpers via `common` (cluster-level rendering,
//! SSA helpers, label / owner-ref builders, status derivation).

pub mod common;
pub mod kafka;
pub mod kafka_node_pool;
pub(crate) mod listeners;
pub(crate) mod metrics;
pub(crate) mod network_policy;
pub mod topic;
```

- [ ] **Step 5: Wire the controller into `run.rs`**

Add the `tokio::spawn` and the corresponding `tokio::select!` arm. The diff for `run.rs`:

```rust
    let kafka_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::kafka::run(ctx).await }
    });
    let pool_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::kafka_node_pool::run(ctx).await }
    });
    let topic_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::topic::run(ctx).await }
    });

    tokio::select! {
        res = health_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "health server exited with error"),
            Err(e) => tracing::error!(error = %e, "health task panicked"),
        },
        res = kafka_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "Kafka controller exited with error"),
            Err(e) => tracing::error!(error = %e, "Kafka controller task panicked"),
        },
        res = pool_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaNodePool controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaNodePool controller task panicked"),
        },
        res = topic_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaTopic controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaTopic controller task panicked"),
        },
        () = shutdown_signal() => tracing::info!("shutdown signal received"),
    }
```

- [ ] **Step 6: Add `KafkaTopic` to `gen_crds.rs::write_all`**

```rust
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
    write_one::<KafkaNodePool>(out_dir)?;
    write_one::<KafkaTopic>(out_dir)?;
    Ok(())
}
```

Also add `use crate::crd::KafkaTopic;` at the top.

Update the test in the same file to assert the new CRD lands:

```rust
    #[test]
    fn writes_kafka_pool_and_topic_crd_files() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        assert!(dir.path().join("crabka.io_kafkas.yaml").exists());
        assert!(dir.path().join("crabka.io_kafkanodepools.yaml").exists());
        assert!(dir.path().join("crabka.io_kafkatopics.yaml").exists());
    }
```

(Remove the previous `writes_kafka_and_pool_crd_files` test in favor of this one.)

- [ ] **Step 7: Create `crates/operator/tests/reconcile_topic.rs`**

The reconcile is async with admin-client side effects. We test it by using the existing `MockState`/`MockRule` kube harness — the admin-client paths are exercised by the `AdminClient` integration test (T1 Step 6), so this file focuses on:

- The kube-side request sequence for each branch (status patches, finalizer patches).
- That admin operations are NOT issued when the spec is invalid (missing label, ClusterNotReady, etc.).

For tests that DO need admin calls, we wire a `Kafka` fixture whose status carries `Ready=True` and whose listener `bootstrap_servers` points at a closed loopback port. The reconcile will then hit `AdminClient::connect` → fail (connection refused) → the reconcile correctly maps the transport failure to a `requeue(15s)` WITHOUT touching the K8s status. We assert exactly that pattern.

```rust
//! Slice 35: reconcile-level tests for the KafkaTopic controller.
//!
//! These tests assert the kube-side request sequence (status patches,
//! finalizer patches). Admin-client behavior is covered by the
//! integration test in `crates/client-admin/tests/round_trip.rs`.

use std::sync::Arc;

use crabka_operator::controller::topic::reconcile;
use crabka_operator::crd::{
    Kafka, KafkaCondition, KafkaSpec, KafkaStatus, KafkaTopic, KafkaTopicSpec,
    ListenerStatus, ListenerType,
};
use http::{Method, Response};
use serde_json::json;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, fake_topic_body, fixture_ctx, json_response, mock_client,
    not_found_body,
};

fn topic(name: &str, ns: &str, cluster: Option<&str>) -> KafkaTopic {
    let mut kt = KafkaTopic::new(
        name,
        KafkaTopicSpec {
            topic_name: None,
            partitions: 3,
            replicas: 1,
            config: None,
            preserve_topic: false,
        },
    );
    kt.metadata.namespace = Some(ns.into());
    kt.metadata.uid = Some("topic-uid".into());
    if let Some(c) = cluster {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), c.into());
        kt.metadata.labels = Some(labels);
    }
    kt
}

/// Slice 35: a KafkaTopic with no `crabka.io/cluster` label must surface
/// `MissingClusterLabel` and issue zero admin RPCs.
#[tokio::test]
async fn missing_cluster_label_sets_status() {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkatopics/foo/status".into(),
        response: json_response(200, &fake_topic_body("foo", "y")),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let kt = topic("foo", "y", None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready");
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "MissingClusterLabel");
}

/// Slice 35: KafkaTopic referencing a Kafka that doesn't exist → status
/// `ClusterNotReady`; no admin RPCs.
#[tokio::test]
async fn cluster_not_found_sets_status_cluster_not_ready() {
    let rules = vec![
        // First, the reconcile validates the topic name (no API call there).
        // Then it GETs the Kafka -> 404.
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/missing-cluster".into(),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("kafka not found"))
                .expect("404 builds"),
        },
        // Status patch.
        MockRule {
            method: Method::PATCH,
            path_substr: "/kafkatopics/foo/status".into(),
            response: json_response(200, &fake_topic_body("foo", "y")),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let kt = topic("foo", "y", Some("missing-cluster"));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .rev()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "ClusterNotReady");
}

/// Slice 35: KafkaTopic whose effective name is invalid (`spec.topicName="."`)
/// → status `InvalidTopicName`; no Kafka GET, no admin RPCs.
#[tokio::test]
async fn invalid_topic_name_sets_status() {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkatopics/foo/status".into(),
        response: json_response(200, &fake_topic_body("foo", "y")),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let mut kt = topic("foo", "y", Some("demo"));
    kt.spec.topic_name = Some(".".into());
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let observed = state.take_observed();
    for r in &observed {
        assert!(
            !r.uri().to_string().contains("/kafkas/"),
            "InvalidTopicName must short-circuit before Kafka GET",
        );
    }
    let status_patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "InvalidTopicName");
}
```

> The reconcile tests above intentionally cover only the kube-API short-circuit paths. The admin-side branches (CreateTopics succeeds / partition increase / config diff / immutable-field) are covered by:
>   - The integration test in `crates/client-admin/tests/round_trip.rs` (exercises every admin RPC end-to-end against an in-process broker).
>   - The renderer-pure unit tests in `controller/topic.rs::tests` (`diff_configs_*`, `validate_topic_name_*`, `internal_listener_bootstrap_*`).
>   - The e2e job (T6) which exercises the full reconcile loop against a kind cluster.

This division avoids the trap of building a fake `AdminClient` mock that has to keep evolving with the surface. The reconcile is thin enough that the kube-API contract + admin-client integration test cover all the meaningful state transitions.

- [ ] **Step 8: Add `fake_topic_body` to `tests/shared/mod.rs`**

Open `crates/operator/tests/shared/mod.rs`, find the existing `fake_kafka_body`, and add:

```rust
pub fn fake_topic_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaTopic",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "topic-uid",
            "generation": 1,
            "finalizers": ["crabka.io/topic-finalizer"],
        },
        "spec": {
            "partitions": 3,
            "replicas": 1,
            "preserveTopic": false,
        },
        "status": {
            "conditions": [],
        }
    })
}
```

- [ ] **Step 9: Build + run tests**

Run: `cargo test -p crabka-operator`
Expected: green; ~6 new lib tests (`validate_topic_name_*`, `diff_configs_*`, `internal_listener_bootstrap_*`) + 3 new reconcile tests.

- [ ] **Step 10: Clippy**

Run: `cargo clippy -p crabka-operator --all-targets -- -D warnings`
Expected: clean. The reconcile fn has `#[allow(clippy::too_many_lines)]` and `patch_status` has `#[allow(clippy::too_many_arguments)]` already; nothing else needed.

- [ ] **Step 11: Commit**

```bash
git add crates/operator/src/controller/topic.rs \
        crates/operator/src/controller/mod.rs \
        crates/operator/src/context.rs \
        crates/operator/src/run.rs \
        crates/operator/src/gen_crds.rs \
        crates/operator/Cargo.toml \
        crates/operator/tests/reconcile_topic.rs \
        crates/operator/tests/shared/mod.rs
git commit -m "Slice 35 T3: KafkaTopic controller + reconcile tests"
```

---

## Task 4 — Helm ClusterRole RBAC

**Files:**
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1: Add the rule block**

Insert the new block immediately AFTER the existing block that grants `kafkanodepools` (keep the two `crabka.io` blocks contiguous):

```yaml
  - apiGroups: ["crabka.io"]
    resources: ["kafkatopics", "kafkatopics/status", "kafkatopics/finalizers"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

- [ ] **Step 2: Lint**

Run: `helm lint charts/crabka-operator`
Expected: 0 errors.

- [ ] **Step 3: Render**

Run: `helm template charts/crabka-operator | grep -A2 kafkatopics`
Expected: the rendered ClusterRole contains the new rule.

- [ ] **Step 4: Commit**

```bash
git add charts/crabka-operator/templates/clusterrole.yaml
git commit -m "Slice 35 T4: ClusterRole grants kafkatopics + finalizers"
```

---

## Task 5 — Regenerate CRD YAML

**Depends on:** T2 + T3 (T3 adds `KafkaTopic` to `gen_crds::write_all`).

**Files:**
- Create (regen): `deploy/crds/crabka.io_kafkatopics.yaml`

- [ ] **Step 1: Regenerate**

Run: `./tools/regen-crds.sh`

Expected: a new `deploy/crds/crabka.io_kafkatopics.yaml` is created. Existing `crabka.io_kafkas.yaml` and `crabka.io_kafkanodepools.yaml` should be unchanged.

- [ ] **Step 2: Inspect the new file**

```
ls deploy/crds/
git status deploy/crds/
```

Expected:
- New: `deploy/crds/crabka.io_kafkatopics.yaml`
- Unchanged: the other two.

- [ ] **Step 3: Confirm idempotence**

Run: `./tools/regen-crds.sh && git status deploy/crds/`

Expected: no further changes (the second regen produces byte-identical files).

- [ ] **Step 4: Commit**

```bash
git add deploy/crds/crabka.io_kafkatopics.yaml
git commit -m "Slice 35 T5: regenerate KafkaTopic CRD YAML"
```

---

## Task 6 — Operator e2e: `kind-kafkatopic` job

**Depends on:** T3 (job asserts the `KafkaTopicReady` status condition).

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Append a new job at the end of the file**

Place it at the same indentation as the existing `kind:` and `kind-network-policy:` jobs:

```yaml
  kind-kafkatopic:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.95.0"
      - uses: azure/setup-helm@v5
        with:
          version: v3.16.2
      - uses: actions/setup-go@v6
        with:
          go-version: stable

      - name: Install melange and apko
        run: |
          go install chainguard.dev/melange@latest
          go install chainguard.dev/apko@latest
          echo "$HOME/go/bin" >> "$GITHUB_PATH"

      - name: Build crabka-operator + crabka-broker images
        run: |
          mkdir -p packages
          melange keygen
          melange build packaging/melange/crabka-operator.yaml \
            --source-dir . --signing-key melange.rsa \
            --arch x86_64 --runner docker --out-dir packages/
          apko build packaging/apko/crabka-operator.yaml \
            crabka-operator:e2e crabka-operator.tar \
            --arch x86_64 \
            --repository-append "$PWD/packages" \
            --keyring-append "$PWD/melange.rsa.pub"
          melange build packaging/melange/crabka-broker.yaml \
            --source-dir . --signing-key melange.rsa \
            --arch x86_64 --runner docker --out-dir packages/
          apko build packaging/apko/crabka-broker.yaml \
            crabka-broker:e2e crabka-broker.tar \
            --arch x86_64 \
            --repository-append "$PWD/packages" \
            --keyring-append "$PWD/melange.rsa.pub"

      - name: Create kind cluster
        uses: helm/kind-action@v1
        with:
          cluster_name: crabka-kt-e2e
          version: v0.24.0
          node_image: kindest/node:v1.30.0

      - name: Load images into kind
        run: |
          set -e
          for tar in crabka-operator.tar crabka-broker.tar; do
            docker load -i "$tar" 2>&1 | tee /tmp/load.log
            loaded=$(sed -n 's/^Loaded image: //p' /tmp/load.log | head -1)
            want=$(basename "$tar" .tar):e2e
            if [ "$loaded" != "$want" ]; then docker tag "$loaded" "$want"; fi
            kind load docker-image "$want" --name crabka-kt-e2e
          done

      - name: Install CRDs + chart
        run: |
          set -e
          kubectl apply -f deploy/crds/crabka.io_kafkas.yaml
          kubectl apply -f deploy/crds/crabka.io_kafkanodepools.yaml
          kubectl apply -f deploy/crds/crabka.io_kafkatopics.yaml
          kubectl create namespace crabka-operator
          helm install operator charts/crabka-operator \
            --namespace crabka-operator \
            --set image.repository=crabka-operator --set image.tag=e2e \
            --set image.pullPolicy=IfNotPresent \
            --set brokerImage.repository=crabka-broker --set brokerImage.tag=e2e \
            --set brokerImage.pullPolicy=IfNotPresent
          kubectl rollout status -n crabka-operator deploy/operator-crabka-operator --timeout=240s

      - name: Apply Kafka cluster + wait Ready
        run: |
          set -e
          cat <<'EOF' | kubectl apply -f -
          apiVersion: crabka.io/v1alpha1
          kind: Kafka
          metadata: { name: demo, namespace: default }
          spec:
            kafkaVersion: "0.1.1"
          ---
          apiVersion: crabka.io/v1alpha1
          kind: KafkaNodePool
          metadata:
            name: brokers
            namespace: default
            labels: { crabka.io/cluster: demo }
          spec:
            roles: [Controller, Broker]
            replicas: 1
            nodeIdStart: 0
          EOF
          for i in $(seq 1 60); do
            s=$(kubectl get kafka demo -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
            echo "attempt $i: Ready=$s"
            if [ "$s" = "True" ]; then break; fi
            sleep 5
          done
          [ "$s" = "True" ] || { echo "::error::Kafka not Ready"; exit 1; }

      - name: Apply KafkaTopic
        run: |
          set -e
          cat <<'EOF' | kubectl apply -f -
          apiVersion: crabka.io/v1alpha1
          kind: KafkaTopic
          metadata:
            name: demo-topic
            namespace: default
            labels: { crabka.io/cluster: demo }
          spec:
            partitions: 3
            replicas: 1
            config:
              retention.ms: "60000"
          EOF

      - name: Wait KafkaTopic Ready
        run: |
          for i in $(seq 1 30); do
            s=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
            r=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)
            echo "attempt $i: status=$s reason=$r"
            if [ "$s" = "True" ]; then exit 0; fi
            sleep 5
          done
          echo "::error::KafkaTopic never Ready"
          kubectl describe kafkatopic demo-topic -n default
          kubectl logs -n crabka-operator deploy/operator-crabka-operator --tail=200 || true
          exit 1

      - name: Assert partition count + config via JVM kafka-topics
        run: |
          set -e
          bootstrap="demo-broker-headless.default.svc.cluster.local:9092"
          kubectl run kt-describe -n default --rm -i --restart=Never \
            --image=apache/kafka:3.8.0 --command -- \
            /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" \
              --describe --topic demo-topic > /tmp/describe.txt
          cat /tmp/describe.txt
          grep -q "PartitionCount: 3" /tmp/describe.txt
          grep -q "ReplicationFactor: 1" /tmp/describe.txt
          kubectl run kt-configs -n default --rm -i --restart=Never \
            --image=apache/kafka:3.8.0 --command -- \
            /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$bootstrap" \
              --describe --entity-type topics --entity-name demo-topic > /tmp/configs.txt
          cat /tmp/configs.txt
          grep -q "retention.ms=60000" /tmp/configs.txt

      - name: Patch to 5 partitions + wait Ready
        run: |
          set -e
          kubectl patch kafkatopic demo-topic -n default --type=merge \
            -p '{"spec":{"partitions":5}}'
          for i in $(seq 1 30); do
            s=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
            g=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.status.observedGeneration}')
            gen=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.metadata.generation}')
            echo "attempt $i: status=$s observed=$g generation=$gen"
            if [ "$s" = "True" ] && [ "$g" = "$gen" ]; then exit 0; fi
            sleep 3
          done
          echo "::error::partition increase never settled"
          exit 1

      - name: Assert 5 partitions visible to JVM tools
        run: |
          set -e
          bootstrap="demo-broker-headless.default.svc.cluster.local:9092"
          kubectl run kt-describe2 -n default --rm -i --restart=Never \
            --image=apache/kafka:3.8.0 --command -- \
            /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" \
              --describe --topic demo-topic > /tmp/describe2.txt
          grep -q "PartitionCount: 5" /tmp/describe2.txt

      - name: Patch to 2 partitions, expect ImmutableFieldChanged
        run: |
          set -e
          kubectl patch kafkatopic demo-topic -n default --type=merge \
            -p '{"spec":{"partitions":2}}'
          for i in $(seq 1 20); do
            r=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)
            echo "attempt $i: reason=$r"
            if [ "$r" = "ImmutableFieldChanged" ]; then exit 0; fi
            sleep 3
          done
          echo "::error::partition decrease did not surface ImmutableFieldChanged"
          kubectl describe kafkatopic demo-topic -n default
          exit 1

      - name: Restore partitions and delete CRD
        run: |
          set -e
          kubectl patch kafkatopic demo-topic -n default --type=merge \
            -p '{"spec":{"partitions":5}}'
          # Wait for Ready=True after restore.
          for i in $(seq 1 20); do
            s=$(kubectl get kafkatopic demo-topic -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
            if [ "$s" = "True" ]; then break; fi
            sleep 3
          done
          kubectl delete kafkatopic demo-topic -n default
          # Wait for topic to actually disappear from Kafka.
          bootstrap="demo-broker-headless.default.svc.cluster.local:9092"
          for i in $(seq 1 20); do
            kubectl run kt-list-$i -n default --rm -i --restart=Never \
              --image=apache/kafka:3.8.0 --command -- \
              /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" \
                --list > /tmp/list.txt
            if ! grep -q "demo-topic" /tmp/list.txt; then exit 0; fi
            sleep 3
          done
          echo "::error::topic still present after CRD delete"
          cat /tmp/list.txt
          exit 1

      - name: Collect diagnostics on failure
        if: failure()
        run: |
          set +e
          mkdir -p /tmp/kt-diag
          {
            echo "## kind-kafkatopic diagnostics"
            kubectl get pods -A -o wide
            echo "### operator logs"
            kubectl logs -n crabka-operator deploy/operator-crabka-operator --tail=500
            echo "### kafka"
            kubectl get kafka demo -n default -o yaml
            echo "### kafkatopic"
            kubectl get kafkatopic demo-topic -n default -o yaml || true
            echo "### broker logs"
            kubectl logs -n default demo-brokers-0 --tail=200 --all-containers || true
          } > /tmp/kt-diag/diagnostics.md

      - name: Upload diagnostics
        if: failure()
        uses: actions/upload-artifact@v7
        with:
          name: operator-e2e-kafkatopic-diagnostics
          path: /tmp/kt-diag/
          retention-days: 14
          if-no-files-found: ignore
```

- [ ] **Step 2: Validate YAML**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml'))"`
Expected: no output (parses cleanly).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/operator-e2e.yml
git commit -m "Slice 35 T6: operator-e2e kind-kafkatopic job"
```

---

## Final verification

After all six tasks land:

- [ ] **Build:**
  Run: `cargo build -p crabka-client-admin -p crabka-operator`
  Expected: clean.

- [ ] **Tests:**
  Run: `cargo test -p crabka-client-admin -p crabka-operator`
  Expected: green. ~5 client-admin unit + 1 integration + existing operator tests + ~9 new operator tests.

- [ ] **Clippy:**
  Run: `cargo clippy --workspace --all-targets -- -D warnings`
  Expected: clean.

- [ ] **CRD regen stability:**
  Run: `./tools/regen-crds.sh && git diff --quiet deploy/crds && echo CLEAN`
  Expected: `CLEAN`.

- [ ] **Helm:**
  Run: `helm lint charts/crabka-operator`
  Expected: 0 errors.

- [ ] **Workflow parse:**
  Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml'))"`
  Expected: no output.

---

## Acceptance criteria (mirrors spec §9)

1. `cargo build` (workspace) clean.
2. `cargo test -p crabka-client-admin` green (5 unit + 1 integration).
3. `cargo test -p crabka-operator` green (existing + 5 CRD-type + 3 reconcile + 6 controller-pure tests).
4. `cargo clippy --workspace --all-targets -- -D warnings` clean.
5. `./tools/regen-crds.sh` idempotent; new `deploy/crds/crabka.io_kafkatopics.yaml` lands.
6. `helm lint charts/crabka-operator` 0 errors.
7. operator-e2e `kind-kafkatopic` job passes the full lifecycle: create → JVM-tool-verified shape → partition increase → JVM-tool verified again → decrease rejected → restore → delete → JVM-tool-verified absence.
