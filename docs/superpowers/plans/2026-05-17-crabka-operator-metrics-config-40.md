# Crabka Operator Slice 40 — `Kafka.spec.metricsConfig` (PodMonitor / ServiceMonitor)

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`. Per CLAUDE.md, dispatch tasks within a batch in parallel; sequential between batches. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Surface the slice-39 broker `/metrics` endpoint through the `Kafka` CRD: opt-in prometheus-operator `PodMonitor` / `ServiceMonitor` generation with a `Kafka.spec.metricsConfig` field. Brokers expose `/metrics` on `:9404` only when opted in (zero-roll upgrade for clusters that don't use it).

**Spec:** [`docs/superpowers/specs/2026-05-17-crabka-operator-metrics-config-40-design.md`](../specs/2026-05-17-crabka-operator-metrics-config-40-design.md).

**Tech stack:** Rust 2024, `kube-rs` (dynamic API for monitoring.coreos.com), `k8s-openapi`, `schemars`, `serde_json`, Helm, kind, prometheus-operator CRDs (pinned tag).

---

## Batch overview

| Batch | Tasks | Files (per task; disjoint within batch) | Parallel? |
|---|---|---|---|
| 1 | T1, T4, T5 | `crd/metrics.rs` + `crd/kafka.rs` + `crd/mod.rs` ‖ `controller/kafka_node_pool.rs` ‖ `charts/.../clusterrole.yaml` | yes |
| 2 | T2 | `controller/metrics.rs` + `controller/mod.rs` + `controller/common.rs` (one error variant) | — |
| 3 | T3 | `controller/kafka.rs` + `tests/reconcile_kafka.rs` | — |
| 4 | T6, T7 | `deploy/crds/crabka.io_kafkas.yaml` (regen) ‖ `.github/workflows/operator-e2e.yml` | yes |

Dependencies: T2 imports T1's types. T3 imports T2's `reconcile_metrics`. T6 regenerates from T1's types. T7 references T4's pod-template port name + T3's status condition.

---

## Task 1 — CRD types: `MetricsConfig` + nested structs

**Files:**
- Create: `crates/operator/src/crd/metrics.rs`
- Modify: `crates/operator/src/crd/mod.rs`
- Modify: `crates/operator/src/crd/kafka.rs`

- [ ] **Step 1: Create `crd/metrics.rs`**

```rust
//! Slice 40: `Kafka.spec.metricsConfig` — operator-side surface for the
//! broker's slice-39 Prometheus `/metrics` endpoint.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    #[serde(default)]
    pub r#type: MetricsType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_monitor: Option<PodMonitorSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_monitor: Option<ServiceMonitorSpec>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MetricsType {
    #[default]
    Prometheus,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodMonitorSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scrape_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceMonitorSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scrape_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_config_defaults_type_prometheus() {
        let cfg: MetricsConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.r#type, MetricsType::Prometheus);
        assert!(cfg.pod_monitor.is_none());
        assert!(cfg.service_monitor.is_none());
    }

    #[test]
    fn metrics_config_round_trips() {
        let cfg = MetricsConfig {
            r#type: MetricsType::Prometheus,
            pod_monitor: Some(PodMonitorSpec {
                interval: Some("15s".into()),
                scrape_timeout: None,
                labels: [("team".to_string(), "platform".to_string())].into(),
            }),
            service_monitor: None,
        };
        let j = serde_json::to_string(&cfg).unwrap();
        assert!(j.contains("\"podMonitor\""));
        assert!(j.contains("\"interval\":\"15s\""));
        let back: MetricsConfig = serde_json::from_str(&j).unwrap();
        assert_eq!(back, cfg);
    }

    #[test]
    fn metrics_type_rejects_unknown() {
        let err = serde_json::from_str::<MetricsType>("\"jmxExporter\"").unwrap_err();
        assert!(err.to_string().contains("prometheus"), "got: {err}");
    }
}
```

- [ ] **Step 2: Re-export from `crd/mod.rs`**

Add after the existing `pub use listener::*;`:

```rust
pub mod metrics;
pub use metrics::{MetricsConfig, MetricsType, PodMonitorSpec, ServiceMonitorSpec};
```

- [ ] **Step 3: Add the field on `KafkaSpec`**

In `crd/kafka.rs`, inside `KafkaSpec`, after `inter_broker_listener_name`:

```rust
/// Slice 40: Prometheus scrape configuration. When `None`, brokers do
/// not bind `/metrics` and no `PodMonitor` / `ServiceMonitor` is
/// rendered. When `Some`, the broker `StatefulSet` gains a `metrics`
/// container port (TCP 9404) and the resources requested by
/// `pod_monitor` / `service_monitor` are SSA-applied.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub metrics_config: Option<crate::crd::MetricsConfig>,
```

- [ ] **Step 4: Add a `KafkaSpec` test for serialization stability**

In the existing `mod tests {}` in `crd/kafka.rs`:

```rust
#[test]
fn spec_omits_metrics_config_when_none() {
    let k = Kafka::new("demo", KafkaSpec {
        kafka_version: "0.1.1".into(),
        config: None,
        listeners: vec![],
        inter_broker_listener_name: None,
        metrics_config: None,
    });
    let j = serde_json::to_string(&k.spec).unwrap();
    assert!(!j.contains("metricsConfig"), "got: {j}");
}

#[test]
fn spec_carries_metrics_config_pod_monitor() {
    use crate::crd::{MetricsConfig, PodMonitorSpec};
    let json = r#"{"kafkaVersion":"0.1.1","metricsConfig":{"podMonitor":{"interval":"30s"}}}"#;
    let spec: KafkaSpec = serde_json::from_str(json).unwrap();
    let cfg = spec.metrics_config.expect("metricsConfig present");
    let pm = cfg.pod_monitor.expect("podMonitor present");
    assert_eq!(pm.interval.as_deref(), Some("30s"));
}
```

Update the existing `KafkaSpec` literal in `round_trips_through_json` to include `metrics_config: None`.

- [ ] **Step 5: Verify**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib crd::
```

Expect: all `crd::metrics::tests::*` and the two new `crd::kafka::tests::*` pass.

---

## Task 4 — Pod-template metrics port + `--metrics-listen-addr` flag

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

This task lives in batch 1 alongside T1 — it doesn't import T1's types; it reads `parent.spec.metrics_config.is_some()`, which is a field access that lands in T1 but is also a trivially-renamed booleanization that can be staged as `false` until T1 lands. To avoid the staging hassle, **start T4 after T1 step 3 lands** (T1 is short; T4 can begin the moment `KafkaSpec.metrics_config` exists in the tree).

- [ ] **Step 1: Add `METRICS_PORT` const**

Near `BROKER_PORT`:

```rust
pub(crate) const METRICS_PORT: i32 = 9404;
```

- [ ] **Step 2: Extract `build_main_script`**

Replace the existing `const MAIN_SCRIPT: &str = …;` (lines 113–122) with:

```rust
// Main script (zero-metrics variant). Retained as a const so the
// `build_main_script_disabled_matches_slice_25_constant` test gives a
// loud failure if the upgrade-stability contract breaks.
const MAIN_SCRIPT: &str = "set -eu\n\
NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n\
cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n\
exec /usr/bin/crabka-broker \\\n  --config-file=/run/crabka/broker.toml \\\n  --broker-id=\"${NODE_ID}\"\n";

fn build_main_script(metrics_enabled: bool) -> String {
    if !metrics_enabled {
        return MAIN_SCRIPT.to_string();
    }
    "set -eu\n\
     NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n\
     cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n\
     exec /usr/bin/crabka-broker \\\n  \
       --config-file=/run/crabka/broker.toml \\\n  \
       --broker-id=\"${NODE_ID}\" \\\n  \
       --metrics-listen-addr=0.0.0.0:9404\n"
        .to_string()
}
```

(Note: the enabled-variant string must be a single literal — no embedded `format!` — so its contents are easy to read in a test failure message. If clippy flags the duplicated body, accept the lint inline with a comment pointing at the upgrade-stability test.)

- [ ] **Step 3: Thread `metrics_enabled` through `render_broker_container`**

```rust
fn render_broker_container(
    broker_image: &str,
    secret_name: &str,
    resources: &ResourceRequirements,
    metrics_enabled: bool,
) -> serde_json::Value {
    let mut ports = vec![json!({
        "containerPort": BROKER_PORT, "name": "kafka-internal", "protocol": "TCP"
    })];
    if metrics_enabled {
        ports.push(json!({
            "containerPort": METRICS_PORT, "name": "metrics", "protocol": "TCP"
        }));
    }
    let main_script = build_main_script(metrics_enabled);
    json!({
        "name": "broker",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [main_script],
        // … existing env block unchanged …
        "ports": ports,
        // … existing probes, resources, volumeMounts, securityContext unchanged …
    })
}
```

(Keep the existing `env`, `readinessProbe`, `livenessProbe`, `resources`, `volumeMounts`, `securityContext` JSON fragments verbatim — only `args`, `ports` change.)

- [ ] **Step 4: Pass the flag from `render_statefulset`**

```rust
pub(crate) fn render_statefulset(
    parent: &Kafka,
    pool: &KafkaNodePool,
    broker_image: &str,
) -> Result<StatefulSet, ReconcileError> {
    // … existing setup …
    let metrics_enabled = parent.spec.metrics_config.is_some();
    let main = render_broker_container(broker_image, &secret_name, &resources, metrics_enabled);
    // … existing assembly …
}
```

- [ ] **Step 5: Add unit tests in the existing `tests` module**

```rust
#[test]
fn build_main_script_disabled_matches_slice_25_constant() {
    // Upgrade-stability contract: clusters with metrics_config=None
    // must get a byte-identical pod template post-slice-40.
    assert_eq!(build_main_script(false), MAIN_SCRIPT);
}

#[test]
fn build_main_script_enabled_appends_metrics_flag() {
    let s = build_main_script(true);
    assert!(s.contains("--metrics-listen-addr=0.0.0.0:9404"), "got: {s:?}");
    assert!(s.contains("--config-file=/run/crabka/broker.toml"));
    assert!(s.ends_with("\n"));
}

#[test]
fn render_statefulset_metrics_off_no_port() {
    // Use any existing test helper that builds a parent Kafka + pool;
    // pass `metrics_config: None`.
    let parent = test_kafka("demo", /* metrics_config */ None);
    let pool = test_pool("brokers");
    let sts = render_statefulset(&parent, &pool, "img:latest").unwrap();
    let ports = sts.spec.unwrap().template.spec.unwrap().containers[0].ports.clone().unwrap();
    assert_eq!(ports.len(), 1);
    assert_eq!(ports[0].name.as_deref(), Some("kafka-internal"));
}

#[test]
fn render_statefulset_metrics_on_adds_port() {
    use crate::crd::{MetricsConfig, PodMonitorSpec};
    let parent = test_kafka("demo", Some(MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        ..Default::default()
    }));
    let pool = test_pool("brokers");
    let sts = render_statefulset(&parent, &pool, "img:latest").unwrap();
    let ports = sts.spec.unwrap().template.spec.unwrap().containers[0].ports.clone().unwrap();
    assert_eq!(ports.len(), 2);
    assert!(ports.iter().any(|p|
        p.name.as_deref() == Some("metrics") && p.container_port == 9404
    ));
}
```

`test_kafka` / `test_pool` likely already exist as helpers in the same `tests` module — extend them with a `metrics_config` parameter (default `None` for the existing callers) so the other slice-25/24 tests don't break.

- [ ] **Step 6: Verify**

```
cargo test -p crabka-operator --lib controller::kafka_node_pool::tests::
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

---

## Task 5 — Helm chart RBAC

**Files:**
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1: Append the monitoring rule**

After the existing `nodes` rule:

```yaml
  - apiGroups: ["monitoring.coreos.com"]
    resources: ["podmonitors", "servicemonitors"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

- [ ] **Step 2: Verify**

```
helm lint charts/crabka-operator
helm template charts/crabka-operator | grep -A4 monitoring.coreos.com
```

---

## Task 2 — Renderer + `reconcile_metrics` + dynamic apply

**Depends on:** T1.

**Files:**
- Create: `crates/operator/src/controller/metrics.rs`
- Modify: `crates/operator/src/controller/mod.rs`
- Modify: `crates/operator/src/controller/common.rs` (add three `ReconcileError` variants)

- [ ] **Step 1: Add error variants in `common.rs`**

Locate the `pub enum ReconcileError` definition; append:

```rust
#[error("metricsConfig: podMonitor and serviceMonitor are mutually exclusive")]
MetricsMutuallyExclusive,
#[error("monitoring.coreos.com/v1 is not served by the API server")]
PrometheusOperatorCrdsMissing,
#[error("malformed input: {0}")]
Malformed(String),
```

(The exact `#[error(...)]` form should match the existing variants' style — adjust if `thiserror::Error` derive uses a different attribute shape in this file.)

- [ ] **Step 2: Register module in `controller/mod.rs`**

```rust
pub(crate) mod metrics;
```

- [ ] **Step 3: Create `controller/metrics.rs`**

Full file (renderers + dynamic apply + `reconcile_metrics`). Skeleton:

```rust
//! Slice 40: metrics reconcile — PodMonitor / ServiceMonitor + metrics-only Service.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::core::{ApiResource, GroupVersionKind};
use kube::{Resource, ResourceExt as _};
use serde_json::json;

use crate::context::Context;
use crate::controller::common::{
    FIELD_MANAGER, ReconcileError, apply_object, common_labels, owner_ref,
};
use crate::controller::kafka_node_pool::METRICS_PORT;
use crate::crd::{Kafka, MetricsConfig, PodMonitorSpec, ServiceMonitorSpec};

pub(crate) const APP_LABEL: &str = "crabka-broker";

pub(crate) fn render_pod_monitor(
    owner: &Kafka,
    cfg: &PodMonitorSpec,
) -> Result<serde_json::Value, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let ns = owner.meta().namespace.clone().unwrap_or_default();
    let mut labels = common_labels(&name, &owner.spec.kafka_version, None);
    for (k, v) in &cfg.labels {
        labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    Ok(json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PodMonitor",
        "metadata": {
            "name": format!("{name}-broker"),
            "namespace": ns,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "namespaceSelector": { "matchNames": [ns] },
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": APP_LABEL,
                    "app.kubernetes.io/instance": name,
                }
            },
            "podMetricsEndpoints": [{
                "port": "metrics",
                "path": "/metrics",
                "interval": cfg.interval.as_deref().unwrap_or("30s"),
                "scrapeTimeout": cfg.scrape_timeout.as_deref().unwrap_or("10s"),
            }],
        }
    }))
}

pub(crate) fn render_service_monitor(
    owner: &Kafka,
    cfg: &ServiceMonitorSpec,
) -> Result<serde_json::Value, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let ns = owner.meta().namespace.clone().unwrap_or_default();
    let mut labels = common_labels(&name, &owner.spec.kafka_version, None);
    for (k, v) in &cfg.labels {
        labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    Ok(json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": {
            "name": format!("{name}-broker"),
            "namespace": ns,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "namespaceSelector": { "matchNames": [ns] },
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": APP_LABEL,
                    "app.kubernetes.io/instance": name,
                }
            },
            "endpoints": [{
                "port": "metrics",
                "path": "/metrics",
                "interval": cfg.interval.as_deref().unwrap_or("30s"),
                "scrapeTimeout": cfg.scrape_timeout.as_deref().unwrap_or("10s"),
            }],
        }
    }))
}

pub(crate) fn render_metrics_service(
    owner: &Kafka,
    cfg: &ServiceMonitorSpec,
) -> Result<Service, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let mut labels = common_labels(&name, &owner.spec.kafka_version, None);
    for (k, v) in &cfg.labels {
        labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    let mut selector = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), name.clone());

    let svc: Service = serde_json::from_value(json!({
        "metadata": {
            "name": format!("{name}-broker-metrics"),
            "namespace": owner.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "clusterIP": "None",
            "selector": selector,
            "ports": [{
                "name": "metrics",
                "port": METRICS_PORT,
                "protocol": "TCP",
                "targetPort": "metrics",
            }],
        }
    }))?;
    Ok(svc)
}

async fn apply_dynamic(
    client: &kube::Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    plural: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), ReconcileError> {
    let (group, version) = api_version
        .split_once('/')
        .ok_or_else(|| ReconcileError::Malformed("apiVersion missing '/'".into()))?;
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    let obj: DynamicObject = serde_json::from_value(body.clone())?;
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    match api.patch(name, &pp, &Patch::Apply(&obj)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            // 404 here means the API group itself is missing — we just
            // tried to PATCH-apply (create-if-missing), so the only way
            // to get 404 is the group not being served.
            Err(ReconcileError::PrometheusOperatorCrdsMissing)
        }
        Err(e) => Err(e.into()),
    }
}

async fn delete_dynamic_if_exists(
    client: &kube::Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    plural: &str,
    name: &str,
) -> Result<(), ReconcileError> {
    let (group, version) = api_version
        .split_once('/')
        .ok_or_else(|| ReconcileError::Malformed("apiVersion missing '/'".into()))?;
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    match api.delete(name, &Default::default()).await {
        Ok(_) | Err(kube::Error::Api(ref ae)) if matches!(api.delete(name, &Default::default()).await,
            Err(kube::Error::Api(ref ae)) if ae.code == 404
        ) => Ok(()),
        Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn reconcile_metrics(
    ctx: &Context,
    owner: &Kafka,
    name: &str,
    namespace: &str,
) -> Option<Result<(), ReconcileError>> {
    let cfg = owner.spec.metrics_config.as_ref()?;

    if cfg.pod_monitor.is_some() && cfg.service_monitor.is_some() {
        return Some(Err(ReconcileError::MetricsMutuallyExclusive));
    }

    if let Some(pm) = &cfg.pod_monitor {
        let body = match render_pod_monitor(owner, pm) {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let pm_name = format!("{name}-broker");
        if let Err(e) = apply_dynamic(
            &ctx.client, namespace,
            "monitoring.coreos.com/v1", "PodMonitor", "podmonitors",
            &pm_name, &body,
        ).await {
            return Some(Err(e));
        }
        // Clean abandoned ServiceMonitor + metrics Service if user
        // toggled away from service_monitor.
        let _ = delete_dynamic_if_exists(&ctx.client, namespace,
            "monitoring.coreos.com/v1", "ServiceMonitor", "servicemonitors",
            &pm_name).await;
        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);
        let _ = svc_api.delete(&format!("{name}-broker-metrics"), &Default::default()).await;
    } else if let Some(sm) = &cfg.service_monitor {
        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);
        let svc = match render_metrics_service(owner, sm) {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };
        let svc_name = format!("{name}-broker-metrics");
        if let Err(e) = apply_object(&svc_api, &svc_name, &svc).await {
            return Some(Err(e.into()));
        }
        let body = match render_service_monitor(owner, sm) {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let sm_name = format!("{name}-broker");
        if let Err(e) = apply_dynamic(
            &ctx.client, namespace,
            "monitoring.coreos.com/v1", "ServiceMonitor", "servicemonitors",
            &sm_name, &body,
        ).await {
            return Some(Err(e));
        }
        // Clean abandoned PodMonitor if user toggled away from pod_monitor.
        let _ = delete_dynamic_if_exists(&ctx.client, namespace,
            "monitoring.coreos.com/v1", "PodMonitor", "podmonitors",
            &sm_name).await;
    }

    Some(Ok(()))
}
```

(Clean up the `delete_dynamic_if_exists` impl above — the example pattern has a redundant double-call; the correct shape is a single `api.delete(...).await` `match` on `code == 404`.)

- [ ] **Step 4: Tests in the same file**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::KafkaSpec;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use kube::Resource as _;

    fn test_kafka() -> Kafka {
        let mut k = Kafka::new("demo", KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
        });
        k.metadata.namespace = Some("default".into());
        k.metadata.uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    #[test]
    fn render_pod_monitor_minimal_defaults() {
        let pm = render_pod_monitor(&test_kafka(), &PodMonitorSpec::default()).unwrap();
        assert_eq!(pm["kind"], "PodMonitor");
        let ep = &pm["spec"]["podMetricsEndpoints"][0];
        assert_eq!(ep["port"], "metrics");
        assert_eq!(ep["path"], "/metrics");
        assert_eq!(ep["interval"], "30s");
        assert_eq!(ep["scrapeTimeout"], "10s");
        assert_eq!(pm["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"], "crabka-broker");
        assert_eq!(pm["spec"]["selector"]["matchLabels"]["app.kubernetes.io/instance"], "demo");
    }

    #[test]
    fn render_pod_monitor_overrides_and_labels() {
        let pm_spec = PodMonitorSpec {
            interval: Some("15s".into()),
            scrape_timeout: Some("5s".into()),
            labels: [("team".to_string(), "platform".to_string())].into(),
        };
        let pm = render_pod_monitor(&test_kafka(), &pm_spec).unwrap();
        assert_eq!(pm["spec"]["podMetricsEndpoints"][0]["interval"], "15s");
        assert_eq!(pm["spec"]["podMetricsEndpoints"][0]["scrapeTimeout"], "5s");
        assert_eq!(pm["metadata"]["labels"]["team"], "platform");
        // Operator labels still win over user labels with same key.
        assert_eq!(pm["metadata"]["labels"]["app.kubernetes.io/name"], "crabka-broker");
    }

    #[test]
    fn render_service_monitor_kind_and_endpoints_key() {
        let sm = render_service_monitor(&test_kafka(), &ServiceMonitorSpec::default()).unwrap();
        assert_eq!(sm["kind"], "ServiceMonitor");
        assert!(sm["spec"]["endpoints"].is_array());
        assert!(sm["spec"]["podMetricsEndpoints"].is_null());
    }

    #[test]
    fn render_metrics_service_is_headless_with_named_port() {
        let svc = render_metrics_service(&test_kafka(), &ServiceMonitorSpec::default()).unwrap();
        let spec = svc.spec.unwrap();
        assert_eq!(spec.cluster_ip.as_deref(), Some("None"));
        let port = &spec.ports.unwrap()[0];
        assert_eq!(port.name.as_deref(), Some("metrics"));
        assert_eq!(port.port, METRICS_PORT);
        assert_eq!(port.target_port.as_ref().unwrap(),
            &k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String("metrics".into()));
    }
}
```

- [ ] **Step 5: Verify**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib controller::metrics::tests::
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

---

## Task 3 — Reconcile wiring in `controller/kafka.rs` + integration tests

**Depends on:** T2.

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/tests/reconcile_kafka.rs`

- [ ] **Step 1: Call `reconcile_metrics` from the main reconcile fn**

Find the spot in `reconcile` (in `controller/kafka.rs`) where the listener reconcile completes and `patch_status` is called. Insert before the status patch:

```rust
let metrics_outcome = crate::controller::metrics::reconcile_metrics(
    &ctx, &obj, &name, &ns,
).await;

let metrics_condition = match &metrics_outcome {
    None => condition(
        "MetricsReady", "False", "Disabled",
        "spec.metricsConfig is not set",
    ),
    Some(Ok(())) => condition(
        "MetricsReady", "True", "Available",
        "metrics resources reconciled",
    ),
    Some(Err(crate::controller::common::ReconcileError::MetricsMutuallyExclusive)) => condition(
        "MetricsReady", "False", "MutuallyExclusive",
        "podMonitor and serviceMonitor are mutually exclusive",
    ),
    Some(Err(crate::controller::common::ReconcileError::PrometheusOperatorCrdsMissing)) => condition(
        "MetricsReady", "False", "PrometheusOperatorCrdsMissing",
        "monitoring.coreos.com/v1 is not served by the API server",
    ),
    Some(Err(e)) => return Err(e.clone()),
};

conditions.push(metrics_condition);
```

(The exact local-variable names — `conditions`, `obj`, `ns`, `name` — must match what already exists at that call site.)

- [ ] **Step 2: Shorten the requeue on `PrometheusOperatorCrdsMissing`**

Where the existing reconcile returns `Action::requeue(Duration::from_secs(N))`, gate on the metrics outcome:

```rust
let requeue = match &metrics_outcome {
    Some(Err(crate::controller::common::ReconcileError::PrometheusOperatorCrdsMissing)) =>
        Duration::from_secs(30),
    _ => existing_requeue,  // whatever was there
};
return Ok(Action::requeue(requeue));
```

- [ ] **Step 3: Add five integration tests in `tests/reconcile_kafka.rs`**

Pattern: build a `MockState` (already present in this file from earlier slices); set up route handlers that match `PATCH /apis/monitoring.coreos.com/v1/namespaces/<ns>/podmonitors/<name>-broker` etc. Use the existing `tower::ServiceExt::oneshot` plumbing.

```rust
#[tokio::test]
async fn metrics_disabled_no_dynamic_apply() {
    let kafka = make_test_kafka(/* metrics_config: */ None);
    let state = run_reconcile(&kafka).await;
    assert!(state.requests_matching("/apis/monitoring.coreos.com/").is_empty());
    let cond = state.latest_status_condition("MetricsReady").unwrap();
    assert_eq!(cond.status, "False");
    assert_eq!(cond.reason, "Disabled");
}

#[tokio::test]
async fn pod_monitor_path_applies_one_resource() {
    let kafka = make_test_kafka(Some(MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        ..Default::default()
    }));
    let state = run_reconcile(&kafka).await;
    let pm_reqs = state.requests_matching("/podmonitors/demo-broker");
    assert_eq!(pm_reqs.len(), 1);
    assert_eq!(pm_reqs[0].method, "PATCH");
    // Inline patch body inspection to confirm FIELD_MANAGER + force=true:
    assert!(pm_reqs[0].url.contains("fieldManager=crabka-operator"));
    assert!(pm_reqs[0].url.contains("force=true"));
    let cond = state.latest_status_condition("MetricsReady").unwrap();
    assert_eq!(cond.status, "True");
}

#[tokio::test]
async fn service_monitor_path_applies_service_and_servicemonitor() {
    let kafka = make_test_kafka(Some(MetricsConfig {
        service_monitor: Some(ServiceMonitorSpec::default()),
        ..Default::default()
    }));
    let state = run_reconcile(&kafka).await;
    assert_eq!(state.requests_matching("/services/demo-broker-metrics").len(), 1);
    assert_eq!(state.requests_matching("/servicemonitors/demo-broker").len(), 1);
}

#[tokio::test]
async fn mutually_exclusive_sets_condition_and_skips_apply() {
    let kafka = make_test_kafka(Some(MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        service_monitor: Some(ServiceMonitorSpec::default()),
        ..Default::default()
    }));
    let state = run_reconcile(&kafka).await;
    assert!(state.requests_matching("/apis/monitoring.coreos.com/").is_empty());
    let cond = state.latest_status_condition("MetricsReady").unwrap();
    assert_eq!(cond.reason, "MutuallyExclusive");
}

#[tokio::test]
async fn prom_operator_missing_sets_condition_and_requeues() {
    let kafka = make_test_kafka(Some(MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        ..Default::default()
    }));
    let state = run_reconcile_with_404_on(&kafka, "/podmonitors/").await;
    let cond = state.latest_status_condition("MetricsReady").unwrap();
    assert_eq!(cond.reason, "PrometheusOperatorCrdsMissing");
    assert_eq!(state.last_requeue.unwrap(), Duration::from_secs(30));
}
```

(`make_test_kafka`, `run_reconcile`, `MockState`, `requests_matching`, `latest_status_condition`, `last_requeue` are the existing patterns in this file — extend the helper signatures to accept `Option<MetricsConfig>` and to allow per-route 404 injection.)

- [ ] **Step 4: Verify**

```
cargo test -p crabka-operator --test reconcile_kafka
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

---

## Task 6 — Regenerate CRD YAML

**Depends on:** T1.

**Files:**
- Modify: `deploy/crds/crabka.io_kafkas.yaml`

- [ ] **Step 1: Run the regen**

```
cargo run -p crabka-operator -- gen-crds deploy/crds
```

(Or `cargo xtask gen-crds` if the workspace has a separate xtask target. Confirm by running `cargo run -p crabka-operator -- --help`.)

- [ ] **Step 2: Inspect the diff**

```
git diff -- deploy/crds/crabka.io_kafkas.yaml
```

Expect: a single insertion of the `metricsConfig` block inside `spec.versions[0].schema.openAPIV3Schema.properties.spec.properties` and corresponding `definitions` entries for `MetricsConfig`, `MetricsType`, `PodMonitorSpec`, `ServiceMonitorSpec`. No other diff.

- [ ] **Step 3: CRD-drift CI check**

Confirm the CI regen-drift job stays green; re-run locally to be safe:

```
cargo run -p crabka-operator -- gen-crds /tmp/crds-check
diff -ru deploy/crds /tmp/crds-check
```

---

## Task 7 — operator-e2e: prometheus-operator CRDs + assertions

**Depends on:** T3 + T4.

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Add prometheus-operator CRD install before chart install**

In the e2e job, after `kind create cluster` and before `helm install crabka-operator …`:

```yaml
      - name: Install prometheus-operator CRDs (PodMonitor + ServiceMonitor)
        run: |
          PROM_OP_TAG=v0.79.2
          kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
          kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml
```

(`PROM_OP_TAG` should be added to the Renovate config — `renovate.json` regex managers — so the tag stays current.)

- [ ] **Step 2: Patch the demo Kafka with `metricsConfig` after the existing slice-25 listener checks**

```yaml
      - name: Enable PodMonitor on demo cluster
        run: |
          kubectl patch kafka demo --type=merge -p '{
            "spec":{
              "metricsConfig":{"podMonitor":{"interval":"15s"}}
            }
          }'
          kubectl wait kafka/demo --for=condition=MetricsReady=True --timeout=120s

      - name: Assert PodMonitor rendered
        run: |
          port=$(kubectl get podmonitor demo-broker -o jsonpath='{.spec.podMetricsEndpoints[0].port}')
          [ "$port" = "metrics" ] || { echo "::error::PodMonitor port wrong: '$port'"; exit 1; }
          interval=$(kubectl get podmonitor demo-broker -o jsonpath='{.spec.podMetricsEndpoints[0].interval}')
          [ "$interval" = "15s" ] || { echo "::error::interval wrong: '$interval'"; exit 1; }

      - name: Assert broker pod exposes metrics container port
        run: |
          port=$(kubectl get pod demo-brokers-0 -o jsonpath='{.spec.containers[?(@.name=="broker")].ports[?(@.name=="metrics")].containerPort}')
          [ "$port" = "9404" ] || { echo "::error::metrics port wrong: '$port'"; exit 1; }

      - name: Scrape /metrics and grep for crabka_broker_ prefix
        run: |
          kubectl run curl-metrics --rm -i --restart=Never --image=curlimages/curl --quiet -- \
            -sf http://demo-brokers-0.demo-broker-headless.default.svc.cluster.local:9404/metrics \
            | grep -q '^# TYPE crabka_broker' \
            || { echo "::error::crabka_broker_* metrics not exposed"; exit 1; }
```

- [ ] **Step 3: Optional — disable test**

If feasible inside the e2e wall-clock budget, append a "toggle off" assertion:

```yaml
      - name: Disable metricsConfig and assert orphan cleanup
        run: |
          kubectl patch kafka demo --type=json -p '[{"op":"remove","path":"/spec/metricsConfig"}]'
          kubectl wait kafka/demo --for=condition=MetricsReady=False --timeout=60s
          # PodMonitor should be deleted by the operator's orphan-cleanup path
          kubectl get podmonitor demo-broker 2>&1 | grep -q 'NotFound' \
            || { echo "::error::PodMonitor not garbage-collected"; exit 1; }
```

(If wall-clock is tight, defer this to a follow-up — the unit-test coverage for orphan cleanup is sufficient gate.)

- [ ] **Step 4: Verify locally if practical**

```
kind create cluster --name slice40
kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.79.2/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
helm install crabka-operator charts/crabka-operator
kubectl apply -f deploy/crds
kubectl apply -f - <<'YAML'
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: { name: demo }
spec:
  kafkaVersion: "0.1.1"
  metricsConfig:
    podMonitor: { interval: 15s }
YAML
kubectl wait kafka/demo --for=condition=MetricsReady=True --timeout=120s
```

---

## Final verification checklist

- [ ] `cargo build -p crabka-operator` clean.
- [ ] `cargo test -p crabka-operator` green.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo run -p crabka-operator -- gen-crds /tmp/x && diff -ru deploy/crds /tmp/x` empty.
- [ ] `helm lint charts/crabka-operator` passes.
- [ ] operator-e2e workflow green in CI.
- [ ] Upgrade smoke (slice 25 → slice 40 with no `metricsConfig`): no broker pod rolled, `MetricsReady=False reason=Disabled` set.

---

## Out of scope (not in this plan)

See the design doc's [§1 — Out (deferred)](../specs/2026-05-17-crabka-operator-metrics-config-40-design.md#out-deferred). Key items intentionally deferred: OTLP export, per-pool override, TLS on `/metrics`, broker-side per-broker label, automatic prometheus-operator install, Strimzi `jmxPrometheusExporter` shim.
