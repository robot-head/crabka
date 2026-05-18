# Slice 35: Operator — `KafkaTopic` CRD + first admin client — Design

**Status:** Approved 2026-05-18.

**Goal:** Add a Strimzi-shaped `KafkaTopic` CRD with **unidirectional** reconciliation against the cluster. CRD is the source of truth: topics are created, partitions increased, configs adjusted, and topics deleted in response to the CRD lifecycle. Out-of-band edits are reverted; immutable-field changes are rejected via status. Ships the first operator-side admin client as a new workspace crate `crates/client-admin`, sized to what Slice 35 needs and extensible for later slices (36 `KafkaUser`, 44 `KafkaRebalance`).

---

## 1. Scope

### In

- New CRD `KafkaTopic` (`crabka.io/v1alpha1`, namespaced, shortname `kt`).
  - `spec.topicName` (optional, defaults to `metadata.name`; validated against Kafka topic-name rules).
  - `spec.partitions` (required, i32, ≥1).
  - `spec.replicas` (required, i32, ≥1).
  - `spec.config` (optional, `BTreeMap<String,String>`).
  - `spec.preserveTopic` (optional, bool, default false).
  - `status.conditions[]`, `status.observedGeneration`, `status.topicName`, `status.topicId`.
- New workspace crate `crates/client-admin` exporting `AdminClient` with: `connect`, `metadata`, `create_topics`, `delete_topics`, `create_partitions`, `incremental_alter_configs`, `describe_configs`. Plaintext only; one-shot `NOT_CONTROLLER` retry; uses `crates/client-core`'s typed `Connection::send<R>` + `ApiVersionTable` negotiation.
- New operator controller `controller::topic`:
  - Watches `KafkaTopic` (primary) and `Kafka` (so a cluster becoming Ready wakes pending topics).
  - Reconciles unidirectionally: CRD wins.
  - Finalizer `crabka.io/topic-finalizer` for delete cascade; `spec.preserveTopic == true` skips `DeleteTopics` but still removes the finalizer.
  - Status condition `Ready` with reasons listed in §2.
- Cluster reference via `metadata.labels["crabka.io/cluster"]=<Kafka name>` (same convention as `KafkaNodePool`). Same namespace.
- Helm `ClusterRole` adds `kafkatopics` (+ `/status`, `/finalizers`) verbs.
- CRD YAML regenerated; `tools/regen-crds.sh` produces `deploy/crds/crabka.io_kafkatopics.yaml`.
- New e2e job `kind-kafkatopic` exercising create / partition-increase / immutable-change rejection / delete via the CRD, verified by JVM `kafka-topics --describe`.

### Out (deferred)

| Concern | Slice / why |
|---|---|
| SASL/SCRAM auth on admin client | Slice 36 wires SCRAM, then 31/36 thread it through |
| TLS on admin client | Phase 4 listener TLS (slice 31) |
| Topic adoption (bidirectional) | Future — operator does not adopt out-of-band topics |
| Replication-factor changes | Slice 43+ (partition reassignment) |
| Partition decreases | Kafka does not support; rejected |
| Per-broker config overrides | Future; topic-level only this slice |
| Topic-naming admission webhook | Future — validation happens at reconcile, surfaced via status |
| Strimzi BTO (Bidirectional Topic Operator) compatibility | Out — unidirectional is the design choice |
| Multi-cluster mirroring through `KafkaTopic` | Future; one topic targets one cluster |

### Constraints inherited

- Crabka is greenfield (CLAUDE.md): no `#[serde(default)]` "for compat", no `V2` variants, no migration shims.
- Apache Kafka wire-protocol byte-exact compatibility for the admin RPCs the admin client emits — this is the constraint that matters. `create_topics`, `metadata`, `create_partitions`, `incremental_alter_configs`, `describe_configs`, `delete_topics` must speak the byte-level shapes the JVM `kafka-topics`/`kafka-configs` admin tools speak, since the e2e differential test asserts equivalence.

---

## 2. CRD shape

New module `crates/operator/src/crd/topic.rs`:

```rust
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
    derive = "PartialEq",
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicSpec {
    /// Optional override for the actual Kafka topic name. Defaults to
    /// `metadata.name`. Validated as a Kafka topic name (length ≤ 249,
    /// chars `[A-Za-z0-9._-]`, not `.` or `..`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Number of partitions. Can be INCREASED at reconcile time via
    /// `CreatePartitions`; decreases are rejected with
    /// `ImmutableFieldChanged`.
    #[schemars(range(min = 1, max = 1_000_000))]
    pub partitions: i32,

    /// Replication factor. Changes are rejected with
    /// `ImmutableFieldChanged` until partition reassignment lands
    /// (slice 43+).
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Opaque topic-level config. Reconciled via
    /// `IncrementalAlterConfigs` — keys present here are SET; keys
    /// dropped relative to the cluster's current overrides are
    /// DELETE'd (revert to broker default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<BTreeMap<String, String>>,

    /// When `true`, the operator skips `DeleteTopics` on CRD delete so
    /// the Kafka topic survives. The finalizer is still removed.
    /// Default `false`.
    #[serde(default)]
    pub preserve_topic: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStatus {
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Effective Kafka topic name (defaulted if `spec.topicName` unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Cluster-assigned topic UUID (from `CreateTopics` response or
    /// `Metadata`). Populated once the topic exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
}
```

### Status conditions

| `Ready.status` | `reason` | meaning |
|---|---|---|
| `True`  | `Ready` | Topic exists and matches spec. |
| `False` | `Pending` | Initial reconcile; in-flight admin call. |
| `False` | `MissingClusterLabel` | `metadata.labels["crabka.io/cluster"]` absent. |
| `False` | `ClusterNotReady` | Target `Kafka` absent, or its `Ready` condition is not `True`. |
| `False` | `InvalidTopicName` | Effective topic name fails Kafka name rules. |
| `False` | `ImmutableFieldChanged` | Partition decrease, replicas change, or `topicName` change. |
| `False` | `BrokerError` | Admin RPC returned a non-recoverable Kafka error code. `message` carries the detail. |

`observedGeneration` advances only when we land a `True/Ready` patch — so a CRD that's stuck on `ImmutableFieldChanged` keeps an older `observedGeneration` until the user fixes the spec.

### `crd/mod.rs` re-export

```rust
pub mod topic;
pub use topic::{KafkaTopic, KafkaTopicSpec, KafkaTopicStatus};
```

---

## 3. New crate `crates/client-admin`

**Why a new crate, not in-operator?** The operator's never opened an admin client. Slice 36 (`KafkaUser`/ACLs) and Slice 44 (`KafkaRebalance`) will both need admin RPCs. Putting the wrapper in a workspace crate now avoids moving code later. The crate is small and additive — no breaking surface to maintain because nothing else depends on it yet.

### Cargo.toml

```toml
[package]
name = "crabka-client-admin"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
crabka-client-core = { workspace = true }
crabka-protocol    = { workspace = true }
bytes              = { workspace = true }
thiserror          = { workspace = true }
tokio              = { workspace = true, features = ["sync"] }
tracing            = { workspace = true }

[dev-dependencies]
crabka-broker      = { workspace = true }
tempfile           = { workspace = true }
tokio              = { workspace = true, features = ["macros", "rt-multi-thread", "test-util"] }
```

Add to workspace `Cargo.toml`:
```toml
[workspace.members]
# … existing …
"crates/client-admin",

[workspace.dependencies]
crabka-client-admin = { path = "crates/client-admin", version = "0.1.1" }
```

### Public API

```rust
use crabka_client_core::Connection;

/// Short-lived admin client targeting one cluster's controller.
/// Plaintext only (TLS / SASL are slice 36 work).
pub struct AdminClient {
    conn: Connection,
}

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("no bootstrap address was reachable: tried {tried}")]
    Connect { tried: usize },
    #[error("controller routing failed after retry")]
    NotControllerExhausted,
    #[error("broker returned error: api={api} code={code} ({name}){detail}",
            detail = .message.as_deref().map(|m| format!(" {m:?}")).unwrap_or_default())]
    Broker { api: &'static str, code: i16, name: &'static str, message: Option<String> },
    #[error("client-core: {0}")]
    Transport(#[from] crabka_client_core::ClientError),
    #[error("protocol: {0}")]
    Protocol(String),
}

impl AdminClient {
    /// Try each bootstrap address in order. First successful connect wins.
    pub async fn connect(bootstrap_addrs: &[String]) -> Result<Self, AdminError>;

    pub async fn metadata(&mut self, topics: &[&str])
        -> Result<TopicMetadata, AdminError>;

    pub async fn create_topics(&mut self, specs: &[CreateTopicSpec], timeout_ms: i32)
        -> Result<Vec<CreateTopicOutcome>, AdminError>;

    pub async fn delete_topics(&mut self, names: &[&str], timeout_ms: i32)
        -> Result<Vec<DeleteTopicOutcome>, AdminError>;

    pub async fn create_partitions(&mut self, ops: &[CreatePartitionsOp], timeout_ms: i32)
        -> Result<Vec<CreatePartitionsOutcome>, AdminError>;

    pub async fn describe_configs(&mut self, topics: &[&str])
        -> Result<Vec<TopicConfigOverrides>, AdminError>;

    pub async fn incremental_alter_configs(&mut self, ops: &[IncrementalAlterOp])
        -> Result<Vec<AlterConfigsOutcome>, AdminError>;
}
```

### Types (exposed to operator)

```rust
pub struct CreateTopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: BTreeMap<String, String>,
}

pub struct CreateTopicOutcome {
    pub name: String,
    pub topic_id: Option<uuid::Uuid>,
    pub error: Option<KafkaError>,
}

pub struct DeleteTopicOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

pub struct CreatePartitionsOp {
    pub name: String,
    pub new_total_count: i32,
}
pub struct CreatePartitionsOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

pub struct TopicMetadata {
    pub topics: Vec<TopicMetadataEntry>,
}
pub struct TopicMetadataEntry {
    pub name: String,
    pub topic_id: Option<uuid::Uuid>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaError>,
}

pub struct TopicConfigOverrides {
    pub topic: String,
    /// Only entries the user / operator has set; broker defaults are
    /// excluded (the broker exposes the source of each entry; we
    /// filter to `DYNAMIC_TOPIC_CONFIG`).
    pub overrides: BTreeMap<String, String>,
}

pub enum IncrementalAlterOp {
    Set { topic: String, key: String, value: String },
    Delete { topic: String, key: String },
}
pub struct AlterConfigsOutcome {
    pub topic: String,
    pub error: Option<KafkaError>,
}

pub struct KafkaError {
    pub code: i16,
    pub name: &'static str,
    pub message: Option<String>,
}
```

### Version negotiation

Each method uses `Connection::send::<TypedRequest>(...)` from `client-core`. The `ApiVersionTable` is auto-populated on `Connection::from_stream()`, so request-version selection is automatic — no per-method hard-coding.

### `NOT_CONTROLLER` retry

`create_topics`, `delete_topics`, `create_partitions`, `incremental_alter_configs` all require routing to the controller in KRaft. The retry pattern:

```rust
async fn call_with_controller_retry<F, Fut, R>(&mut self, mut op: F) -> Result<R, AdminError>
where
    F: FnMut(&mut Connection) -> Fut,
    Fut: Future<Output = Result<R, AdminError>>,
{
    match op(&mut self.conn).await {
        Ok(r) => Ok(r),
        Err(AdminError::Broker { code: NOT_CONTROLLER, .. }) => {
            // Refresh Metadata, find the controller, reconnect once.
            let md = metadata::raw(&mut self.conn, &[]).await?;
            let host = md.controller_endpoint()
                .ok_or(AdminError::NotControllerExhausted)?;
            self.conn = Connection::connect(&host).await?;
            op(&mut self.conn).await
        }
        Err(e) => Err(e),
    }
}
```

Two retries max — after a second `NOT_CONTROLLER` we return `NotControllerExhausted` and let the reconcile requeue handle it.

### Plaintext-only

`AdminClient::connect` opens a TCP connection via `Connection::connect(&addr)`. No TLS, no SASL. Slice 36 will extend with a connect-config struct that carries auth material.

---

## 4. Controller: `controller::topic`

New module `crates/operator/src/controller/topic.rs`.

### Watches

```rust
Controller::new(topic_api, watcher::Config::default())
    // Kafka watch fires the controller's reconcile loop but the mapper
    // returns empty: listing matching topics inside the sync `mapper`
    // closure would require an async kube list call which the
    // `watches` signature doesn't allow, and a `futures::executor::block_on`
    // panics inside a tokio runtime. The 60-second periodic requeue on
    // each `KafkaTopic` catches cluster Ready transitions in time for
    // operator UX; sub-minute responsiveness is not a slice-35 goal.
    .watches(kafka_api, watcher::Config::default(), |_kafka| {
        Vec::<ObjectRef<KafkaTopic>>::new().into_iter()
    })
    .run(reconcile, error_policy, Arc::new(ctx))
```

The same approach is used in `controller::kafka::run` for its `Node` watch — see the comment in `crates/operator/src/controller/kafka.rs` for the precedent. Sub-minute wake-up on cluster readiness is deferred to a future slice that wires a reflector cache through `TopicContext`.

### Reconcile fn (full pseudocode)

```rust
async fn reconcile(obj: Arc<KafkaTopic>, ctx: Arc<TopicContext>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let topic_api: Api<KafkaTopic> = Api::namespaced(ctx.client.clone(), &ns);

    // 1. Cluster label
    let cluster = obj.meta().labels.as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned());
    let Some(cluster) = cluster else {
        return patch_topic_status(&topic_api, &name, &obj,
            Ready::False, "MissingClusterLabel",
            "metadata.labels[\"crabka.io/cluster\"] is required",
            None, Duration::from_secs(60)).await;
    };

    // 2. Resolve Kafka + bootstrap address
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let kafka = kafka_api.get_opt(&cluster).await?;
    let bootstrap = kafka.as_ref().and_then(internal_listener_bootstrap);
    let Some(bootstrap) = bootstrap else {
        return patch_topic_status(&topic_api, &name, &obj,
            Ready::False, "ClusterNotReady",
            &format!("Kafka/{cluster} is not Ready or has no internal listener"),
            None, Duration::from_secs(30)).await;
    };

    // 3. Effective topic name + validation
    let topic_name = obj.spec.topic_name.clone().unwrap_or_else(|| name.clone());
    if let Err(msg) = validate_kafka_topic_name(&topic_name) {
        return patch_topic_status(&topic_api, &name, &obj,
            Ready::False, "InvalidTopicName", &msg, None,
            Duration::from_secs(300)).await;
    }

    // 4. Finalizer / delete path
    if obj.meta().deletion_timestamp.is_some() {
        if !obj.spec.preserve_topic {
            let mut admin = ctx.admin_client_for(&cluster, &bootstrap).await?;
            // Best-effort: a 404-equivalent error is logged but not propagated.
            let _ = admin.delete_topics(&[&topic_name], 30_000).await;
        }
        remove_finalizer(&topic_api, &name).await?;
        return Ok(Action::await_change());
    }

    // 5. Ensure finalizer present
    if !has_finalizer(&obj, FINALIZER) {
        add_finalizer(&topic_api, &name, FINALIZER).await?;
        return Ok(Action::requeue(Duration::from_secs(0))); // re-enter
    }

    // 6. Fetch current cluster-side state
    let mut admin = ctx.admin_client_for(&cluster, &bootstrap).await?;
    let md = admin.metadata(&[&topic_name]).await?;
    let current = md.topics.iter().find(|t| t.name == topic_name);

    match current {
        None => {
            // CreateTopics
            let result = admin.create_topics(
                &[CreateTopicSpec {
                    name: topic_name.clone(),
                    partitions: obj.spec.partitions,
                    replicas: obj.spec.replicas,
                    configs: obj.spec.config.clone().unwrap_or_default(),
                }],
                30_000,
            ).await?;
            let outcome = result.into_iter().next().expect("one spec → one outcome");
            if let Some(err) = outcome.error {
                return patch_topic_status(&topic_api, &name, &obj,
                    Ready::False, "BrokerError",
                    &format!("CreateTopics failed: {} ({})", err.name, err.code),
                    None, Duration::from_secs(15)).await;
            }
            patch_topic_status(&topic_api, &name, &obj,
                Ready::True, "Ready", "topic created",
                outcome.topic_id.map(|u| u.to_string()),
                Duration::from_secs(60),
            ).await
        }
        Some(cur) => {
            // Immutable-field validation
            if cur.replication_factor != obj.spec.replicas {
                return patch_topic_status(&topic_api, &name, &obj,
                    Ready::False, "ImmutableFieldChanged",
                    "spec.replicas change requires partition reassignment (slice 43+)",
                    cur.topic_id.map(|u| u.to_string()),
                    Duration::from_secs(300)).await;
            }
            if cur.partition_count > obj.spec.partitions {
                return patch_topic_status(&topic_api, &name, &obj,
                    Ready::False, "ImmutableFieldChanged",
                    "spec.partitions decrease is not supported by Kafka",
                    cur.topic_id.map(|u| u.to_string()),
                    Duration::from_secs(300)).await;
            }

            // Partition increase
            if cur.partition_count < obj.spec.partitions {
                let outcome = admin.create_partitions(
                    &[CreatePartitionsOp {
                        name: topic_name.clone(),
                        new_total_count: obj.spec.partitions,
                    }],
                    30_000,
                ).await?.into_iter().next().expect("one op → one outcome");
                if let Some(err) = outcome.error {
                    return patch_topic_status(&topic_api, &name, &obj,
                        Ready::False, "BrokerError",
                        &format!("CreatePartitions failed: {} ({})", err.name, err.code),
                        cur.topic_id.map(|u| u.to_string()),
                        Duration::from_secs(15)).await;
                }
            }

            // Config diff
            let desired = obj.spec.config.clone().unwrap_or_default();
            let overrides = admin.describe_configs(&[&topic_name]).await?
                .into_iter().next().map(|t| t.overrides).unwrap_or_default();
            let ops = diff_configs(&overrides, &desired, &topic_name);
            if !ops.is_empty() {
                let outcomes = admin.incremental_alter_configs(&ops).await?;
                if let Some(err) = outcomes.into_iter().find_map(|o| o.error) {
                    return patch_topic_status(&topic_api, &name, &obj,
                        Ready::False, "BrokerError",
                        &format!("IncrementalAlterConfigs failed: {} ({})", err.name, err.code),
                        cur.topic_id.map(|u| u.to_string()),
                        Duration::from_secs(15)).await;
                }
            }

            patch_topic_status(&topic_api, &name, &obj,
                Ready::True, "Ready", "topic in sync",
                cur.topic_id.map(|u| u.to_string()),
                Duration::from_secs(60),
            ).await
        }
    }
}
```

### `diff_configs` helper

Pure, in `controller/topic.rs`:

```rust
fn diff_configs(
    current: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
    topic: &str,
) -> Vec<IncrementalAlterOp> {
    let mut ops = Vec::new();
    for (k, v) in desired {
        if current.get(k) != Some(v) {
            ops.push(IncrementalAlterOp::Set { topic: topic.into(), key: k.clone(), value: v.clone() });
        }
    }
    for k in current.keys() {
        if !desired.contains_key(k) {
            ops.push(IncrementalAlterOp::Delete { topic: topic.into(), key: k.clone() });
        }
    }
    ops
}
```

Tested standalone — bytes-deterministic given a fixed iteration order on `BTreeMap`.

### `internal_listener_bootstrap`

```rust
fn internal_listener_bootstrap(kafka: &Kafka) -> Option<String> {
    let ready_true = kafka.status.as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"))
        .is_some_and(|c| c.status == "True");
    if !ready_true { return None; }
    let inter_broker = kafka.spec.inter_broker_listener_name.as_deref().unwrap_or("PLAIN");
    kafka.status.as_ref()?
        .listeners.iter()
        .find(|l| l.name == inter_broker)
        .map(|l| l.bootstrap_servers.clone())
        .filter(|s| !s.is_empty())
}
```

### Kafka topic name validation

```rust
fn validate_kafka_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() { return Err("topic name is empty".into()); }
    if name.len() > 249 { return Err(format!("topic name length {} exceeds 249", name.len())); }
    if name == "." || name == ".." { return Err("topic name cannot be \".\" or \"..\"".into()); }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-') {
        return Err(format!("topic name {name:?} contains invalid characters"));
    }
    Ok(())
}
```

(Kafka's actual rule is `[a-zA-Z0-9._\-]+` plus the length cap; the `.`/`..` check protects against legacy ZK-era foot-guns. We mirror the JVM client's `Topic.validate`.)

### `TopicContext`

A new small struct distinct from `Context`. It holds the kube `Client` PLUS a `Mutex<HashMap<String, Arc<Mutex<AdminClient>>>>` keyed by cluster name. The `admin_client_for` method:

1. Looks up an existing client for the cluster.
2. If absent or the connection is broken, opens a new one against `bootstrap_addr` and inserts.
3. Returns a guard.

Connections are dropped at process exit; no TTL — broken connections re-open on the next call. This is small and safe; replace with a richer pool later if needed.

```rust
pub struct TopicContext {
    pub client: kube::Client,
    pub admin_clients: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<AdminClient>>>>,
}
```

### Wiring in `run.rs`

```rust
tokio::spawn({
    let ctx = topic_ctx.clone();
    async move { controller::topic::run(ctx).await }
});
```

Logged-error select arm mirrors the existing two controllers.

### Finalizer constant

```rust
const FINALIZER: &str = "crabka.io/topic-finalizer";
```

---

## 5. Helm chart RBAC additions

`charts/crabka-operator/templates/clusterrole.yaml` gains:

```yaml
  - apiGroups: ["crabka.io"]
    resources: ["kafkatopics", "kafkatopics/status", "kafkatopics/finalizers"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

No `values.yaml` change.

---

## 6. Testing

### `crates/client-admin/tests` and unit tests

**Unit (`src/topics.rs::tests`, `src/configs.rs::tests`, `src/lib.rs::tests`):**
- `create_topics_one_spec_round_trip` — encode + decode against a mock Connection.
- `metadata_request_default_topics_returns_all` — `topics: &[]` triggers the protocol's "all topics" semantics.
- `not_controller_triggers_one_retry` — mock Connection returns `NOT_CONTROLLER` then succeeds; assert the retry path.
- `repeated_not_controller_errors_return_exhausted` — two consecutive `NOT_CONTROLLER`s → `AdminError::NotControllerExhausted`.
- `connect_walks_bootstrap_list` — first address refuses, second accepts; assert the second is used.
- `describe_configs_filters_to_dynamic_topic` — broker returns mixed `STATIC_BROKER_CONFIG` + `DYNAMIC_TOPIC_CONFIG`; assert we keep only the dynamic-topic entries.

**Integration (`tests/round_trip.rs`):**
Uses `crates/broker/tests/support::start()` to spawn an in-process broker. Round-trips:

```text
let support = support::start().await;
let bootstrap = support.bootstrap_addr.to_string();
let mut admin = AdminClient::connect(&[bootstrap]).await?;

admin.metadata(&["foo"]).await? // -> Some(error=UnknownTopicOrPartition)
admin.create_topics(&[CreateTopicSpec{ name: "foo", partitions: 3, replicas: 1, configs: ... }], 5_000).await?;
admin.metadata(&["foo"]).await? // -> exists, p=3, rf=1
admin.create_partitions(&[CreatePartitionsOp{ name: "foo", new_total_count: 5 }], 5_000).await?;
admin.metadata(&["foo"]).await? // -> p=5
admin.incremental_alter_configs(&[IncrementalAlterOp::Set{ "foo", "retention.ms", "60000" }]).await?;
admin.describe_configs(&["foo"]).await? // -> retention.ms=60000
admin.delete_topics(&["foo"], 5_000).await?;
admin.metadata(&["foo"]).await? // -> UnknownTopicOrPartition
```

### `crates/operator/src/crd/topic.rs::tests`

- `topic_spec_round_trips` — full struct → JSON → struct.
- `spec_omits_optional_fields_when_default` — defaults serialize cleanly.
- `status_topic_id_omitted_when_none` — `topic_id: None` → no JSON key.
- `crd_metadata_is_correct` — shortname `kt`, plural `kafkatopics`.

### `crates/operator/src/controller/topic.rs::tests` (renderer-pure + helper-pure)

- `validate_topic_name_*` — accept, length, charset, dot, empty cases.
- `diff_configs_set_adds_missing_key` — desired has key, current doesn't → `Set`.
- `diff_configs_set_updates_changed_value` — both have key, values differ → `Set`.
- `diff_configs_delete_removes_extra_key` — current has key, desired doesn't → `Delete`.
- `diff_configs_noop_when_matching` — equal maps → empty Vec.
- `internal_listener_bootstrap_picks_inter_broker_listener` — fixture Kafka with `Ready=True` and matching `inter_broker_listener_name` → returns its bootstrap.
- `internal_listener_bootstrap_returns_none_when_kafka_not_ready`.

### `crates/operator/tests/reconcile_topic.rs`

Mock kube client (existing harness) + a stub admin client. Approach: replace `TopicContext::admin_client_for` with a test seam — define `trait AdminClientLike` covering the 6 methods we call, and have `TopicContext` hold a boxed dyn. Production wires the real `AdminClient`; tests wire a fixture that records calls and returns canned responses.

Tests:
- `missing_cluster_label` — no label → status `MissingClusterLabel`, zero admin calls.
- `cluster_not_ready` — Kafka not Ready → `ClusterNotReady`, zero admin calls.
- `creates_topic_on_first_reconcile` — fixture admin returns "not found" then "created" → one `CreateTopics`, status `Ready=True` with `topic_id` set.
- `noop_when_spec_matches_cluster` — admin returns matching metadata + empty config diff → no mutating calls, status `Ready=True`.
- `partition_increase_triggers_create_partitions`.
- `partition_decrease_sets_immutable_field_changed`.
- `replicas_change_sets_immutable_field_changed`.
- `config_diff_sets_and_deletes` — combine SET + DELETE in one call.
- `delete_with_finalizer_calls_delete_topics`.
- `delete_with_preserve_topic_skips_delete_topics`.
- `invalid_topic_name_sets_status`.

### E2E (`.github/workflows/operator-e2e.yml` — new `kind-kafkatopic` job)

Standard kind cluster, no Calico (we just need apiserver + broker + JVM client).

1. Build operator + broker images (existing pattern).
2. Boot kind, load images, install CRDs + chart.
3. Apply Kafka + KafkaNodePool, wait `Kafka/demo Ready=True`.
4. Apply:
   ```yaml
   apiVersion: crabka.io/v1alpha1
   kind: KafkaTopic
   metadata:
     name: demo-topic
     namespace: default
     labels:
       crabka.io/cluster: demo
   spec:
     partitions: 3
     replicas: 1
     config:
       retention.ms: "60000"
   ```
5. `kubectl wait kafkatopic/demo-topic --for=condition=Ready --timeout=60s`.
6. Run a JVM `kafka-topics --describe --topic demo-topic --bootstrap-server <bootstrap>`. Assert:
   - Partition count 3.
   - RF 1.
   - `retention.ms=60000` (use `kafka-configs --describe --entity-type topics --entity-name demo-topic`).
7. `kubectl patch kafkatopic/demo-topic --type=merge -p '{"spec":{"partitions":5}}'`.
8. Wait Ready again; assert JVM tools see 5 partitions.
9. `kubectl patch kafkatopic/demo-topic --type=merge -p '{"spec":{"partitions":2}}'`.
10. Wait `Ready=False reason=ImmutableFieldChanged` (10s should suffice).
11. `kubectl patch kafkatopic/demo-topic --type=merge -p '{"spec":{"partitions":5}}'` (revert).
12. `kubectl delete kafkatopic/demo-topic`.
13. Poll `kafka-topics --list` until `demo-topic` is gone.

Diagnostic upload step mirrors existing jobs.

---

## 7. File structure

```
crates/client-admin/                              # NEW
├── Cargo.toml
└── src/
    ├── lib.rs                                    # AdminClient + AdminError + connect + retry helper
    ├── topics.rs                                 # metadata + create_topics + delete_topics + create_partitions
    ├── configs.rs                                # describe_configs + incremental_alter_configs + diff helpers
    └── tests/
        └── round_trip.rs                         # integration test against support::start()

crates/operator/src/crd/
├── topic.rs                                      # NEW
├── mod.rs                                        # MODIFIED — re-export

crates/operator/src/controller/
├── topic.rs                                      # NEW
├── mod.rs                                        # MODIFIED — pub(crate) mod topic

crates/operator/src/
├── context.rs                                    # MODIFIED — TopicContext + admin_client_for cache
├── run.rs                                        # MODIFIED — tokio::spawn the new controller
├── gen_crds.rs                                   # MODIFIED — write_one::<KafkaTopic>

crates/operator/tests/
├── reconcile_topic.rs                            # NEW

charts/crabka-operator/templates/
├── clusterrole.yaml                              # MODIFIED — kafkatopics + finalizers + status verbs

deploy/crds/
├── crabka.io_kafkatopics.yaml                    # NEW (generated)

Cargo.toml (workspace)                            # MODIFIED — new member + workspace dep

.github/workflows/operator-e2e.yml                # MODIFIED — kind-kafkatopic job
```

---

## 8. Conflict analysis (for parallel batching)

| File | Tasks touching it |
|---|---|
| `crates/client-admin/**` | T1 |
| `crates/operator/src/crd/topic.rs` | T2 |
| `crates/operator/src/crd/mod.rs` | T2 |
| `crates/operator/src/controller/topic.rs` | T3 |
| `crates/operator/src/controller/mod.rs` | T3 |
| `crates/operator/src/context.rs` | T3 |
| `crates/operator/src/run.rs` | T3 |
| `crates/operator/src/gen_crds.rs` | T3 (or T4 if regen is split) |
| `crates/operator/tests/reconcile_topic.rs` | T3 |
| `charts/crabka-operator/templates/clusterrole.yaml` | T4 |
| `deploy/crds/crabka.io_kafkatopics.yaml` | T5 |
| `Cargo.toml` (workspace) | T1 |
| `.github/workflows/operator-e2e.yml` | T6 |

Parallel batches:

- **Batch 1:** T1 (`crates/client-admin` crate) ‖ T2 (CRD type) ‖ T4 (Helm RBAC). All disjoint.
- **Batch 2:** T3 (controller + wire-up + reconcile tests). Depends on T1 + T2.
- **Batch 3:** T5 (CRD regen) ‖ T6 (e2e workflow). Disjoint.

Roughly: T1 ‖ T2 ‖ T4 → T3 → T5 ‖ T6.

---

## 9. Acceptance criteria

1. `cargo build` (workspace) clean.
2. `cargo test -p crabka-client-admin` green (~6 unit + 1 integration).
3. `cargo test -p crabka-operator` green (existing + ~12 new tests).
4. `cargo clippy --workspace --all-targets -- -D warnings` clean.
5. `./tools/regen-crds.sh` produces no diff after first run; `deploy/crds/crabka.io_kafkatopics.yaml` is generated.
6. `helm lint charts/crabka-operator` 0 errors.
7. `kind-kafkatopic` e2e job passes: CRD create → partition increase → immutable-change rejected → delete cascade, all assertions through JVM `kafka-topics` / `kafka-configs`.

---

## 10. Open questions resolved

- **Where does the admin client live?** New `crates/client-admin` workspace member. Slice 36 (`KafkaUser`) and 44 (`KafkaRebalance`) will extend it.
- **Reconcile direction?** Unidirectional. CRD is source of truth.
- **Out-of-band changes?** Reverted on next reconcile. Topic-deleted-out-of-band is recreated.
- **Immutable-field policy?** Reject and surface in `Ready=False reason=ImmutableFieldChanged`. No admission webhook in this slice.
- **Delete cascade?** Finalizer-based; `spec.preserveTopic` opts out of the `DeleteTopics` call.
- **`spec.topicName`?** Optional, defaults to `metadata.name`.
- **`spec.partitions` / `spec.replicas` required?** Yes. Schema `range(min=1, ...)`.
- **Expose `topic_id`?** Yes, in status — useful for debugging and Strimzi parity.
- **Auth?** Plaintext for now; Slice 36 will add SCRAM via an `AdminClientConfig` extension.
- **`DescribeConfigs` in the admin surface?** Yes — required for idempotent config-diff reconcile.
- **`NOT_CONTROLLER` retry?** One in-client retry max; subsequent failures requeue.
- **Per-cluster connection cache?** Yes, keyed by cluster name. Broken connections reopen on next reconcile.
- **Bootstrap source?** `Kafka.status.listeners[<inter_broker_listener_name>].bootstrap_servers`, gated on `Kafka.status.conditions[Ready].status == "True"`.
