# Slice 40: Operator — `Kafka.spec.metricsConfig` (PodMonitor / ServiceMonitor) — Design

**Status:** Approved 2026-05-17.

**Goal:** Surface the slice-39 broker `/metrics` endpoint through the `Kafka` CRD so a cluster operator can opt the brokers into Prometheus scraping via prometheus-operator CRDs (`PodMonitor` or `ServiceMonitor`) without writing the resources by hand. Slice 40 is the operator-side surfacing of slice 39 — no broker change.

---

## 1. Scope

### In

- `KafkaSpec.metrics_config: Option<MetricsConfig>` with a `prometheus` type discriminator and two mutually-exclusive scrape modes:
  - `pod_monitor` → renders one `monitoring.coreos.com/v1 PodMonitor` per cluster targeting broker pods directly.
  - `service_monitor` → renders one cluster-scoped metrics-only `Service` (`<cluster>-broker-metrics`) plus one `ServiceMonitor` that selects it.
- When `metrics_config` is `Some`, the per-pool `StatefulSet` pod template gains:
  - A second `containerPort` named `metrics` (TCP 9404) on the broker container.
  - `--metrics-listen-addr=0.0.0.0:9404` appended to `MAIN_SCRIPT`.
- When `metrics_config` is `None`, the pod template is byte-identical to slice 25 — no roll on upgrade for clusters that don't opt in.
- New status condition `MetricsReady` on `KafkaStatus.conditions`, with reasons:
  - `Disabled` (status `False`) — `metrics_config` unset; informational, not an error.
  - `Available` (status `True`) — resources reconciled successfully.
  - `MutuallyExclusive` (status `False`) — both `pod_monitor` and `service_monitor` set.
  - `PrometheusOperatorCrdsMissing` (status `False`) — apply hit 404 on the prometheus-operator API group; operator requeues with backoff.
- Helm chart RBAC additions for `monitoring.coreos.com` (verbs `get,list,watch,create,update,patch,delete` on `podmonitors`, `servicemonitors`).
- CRD YAML regenerated; `cargo xtask gen-crds` clean.
- Unit reconcile tests; one operator-e2e job that installs the prometheus-operator CRDs (CRD YAML only, not the controller) and asserts the rendered `PodMonitor` body.

### Out (deferred)

| Concern | Slice / why |
|---|---|
| OpenTelemetry / OTLP metrics export | Phase 6 follow-up to slice 42 (OTLP tracing) |
| Per-pool `metricsConfig` override (cluster-wide today) | future |
| Operator-managed Prometheus / Alertmanager / Grafana stack | out — `metricsConfig` only generates `monitoring.coreos.com/v1` resources |
| Per-broker label on emitted metrics (the broker exporter does not yet add `broker_id`) | slice-39 follow-up; not blocking |
| `metricsConfig.type=jmxPrometheusExporter` Strimzi compatibility (no JVM, no JMX) | rejected at the schema level via the `MetricsType` enum |
| TLS-protected `/metrics` endpoint | future; broker exposes plaintext today |
| Authentication on the `/metrics` endpoint | future (Kafka convention is unauth; relies on K8s network policy) |
| Histograms / per-API-key request counters | slice-39 follow-up |
| Operator-self metrics already shipped via slice 17 — unchanged | n/a |

### Constraints inherited

- Crabka is greenfield: no `serde(default)` backwards-compat shims or `V2` enum variants. `metrics_config: Option<…>` defaulting to `None` is the only "compat" needed.
- slice-21 config-hash drives rolling restart on `spec.config` / listener-intent change only. Pod-template changes (metrics port + CLI flag) are handled by the StatefulSet controller's own template-change roll (graceful via slice-22 controlled-shutdown + readiness probes). No change to the slice-21 hash function.

---

## 2. CRD shape

New module `crates/operator/src/crd/metrics.rs`:

```rust
use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cluster-level metrics surface. When set, the broker StatefulSets
/// expose `/metrics` on port 9404 and the operator generates
/// prometheus-operator scrape resources.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    /// Source of metrics. The only valid value is `prometheus` — Crabka
    /// has no JVM and no JMX exporter to choose between. The
    /// discriminator is retained so a future OTLP slice can extend the
    /// enum without a breaking schema change.
    #[serde(default)]
    pub r#type: MetricsType,

    /// PodMonitor-based scrape. Mutually exclusive with `service_monitor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_monitor: Option<PodMonitorSpec>,

    /// ServiceMonitor-based scrape backed by a generated metrics-only
    /// headless `Service`. Mutually exclusive with `pod_monitor`.
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
    /// Scrape interval, Prometheus duration string. Default `30s`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    /// Per-scrape timeout. Default `10s`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scrape_timeout: Option<String>,
    /// Extra labels on the `PodMonitor` metadata. Common use: match
    /// `Prometheus.spec.podMonitorSelector` in the cluster's
    /// prometheus-operator config.
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
    /// Labels on both the generated `Service` and the `ServiceMonitor`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}
```

Add to `KafkaSpec`:

```rust
/// Slice 40: Prometheus scrape configuration. When `None`, brokers do
/// not bind `/metrics` and no `PodMonitor` / `ServiceMonitor` is
/// rendered. When `Some`, the broker `StatefulSet` gains a `metrics`
/// container port (TCP 9404) and the resources requested by
/// `pod_monitor` / `service_monitor` are SSA-applied.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub metrics_config: Option<crate::crd::MetricsConfig>,
```

Add to `crd/mod.rs`:

```rust
pub mod metrics;
pub use metrics::{MetricsConfig, MetricsType, PodMonitorSpec, ServiceMonitorSpec};
```

### Validation rules (status condition `MetricsReady`)

| Condition | Reason | Resources rendered? |
|---|---|---|
| `metrics_config` unset | `Disabled` | none — pod template unchanged |
| `pod_monitor` and `service_monitor` both set | `MutuallyExclusive` | none — existing resources untouched |
| Apply on `monitoring.coreos.com/v1` returns 404 (group not served) | `PrometheusOperatorCrdsMissing` | none — requeue with backoff |
| Otherwise | `Available` | as requested |

`Disabled` is intentionally surfaced as `MetricsReady=False reason=Disabled`, not as the condition's absence — this lets `kubectl wait --for=condition=MetricsReady` distinguish "not configured" from "configuring" without polling forever on an unconfigured cluster. Operators that want a healthy-or-disabled gate look at `MetricsReady ∈ {True, False reason=Disabled}`.

---

## 3. Pod-template changes (`controller/kafka_node_pool.rs`)

A new pub-crate const:

```rust
pub(crate) const METRICS_PORT: i32 = 9404;
```

`render_broker_container` becomes:

```rust
fn render_broker_container(
    broker_image: &str,
    secret_name: &str,
    resources: &ResourceRequirements,
    metrics_enabled: bool,                          // NEW
) -> serde_json::Value {
    let mut ports = vec![json!({
        "containerPort": BROKER_PORT,
        "name": "kafka-internal",
        "protocol": "TCP",
    })];
    if metrics_enabled {
        ports.push(json!({
            "containerPort": METRICS_PORT,
            "name": "metrics",
            "protocol": "TCP",
        }));
    }
    let main_script = build_main_script(metrics_enabled);

    json!({
        "name": "broker",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [main_script],
        // … existing env + probes + volumeMounts + securityContext …
        "ports": ports,
        // …
    })
}

fn build_main_script(metrics_enabled: bool) -> String {
    let metrics_flag = if metrics_enabled {
        " \\\n  --metrics-listen-addr=0.0.0.0:9404"
    } else {
        ""
    };
    format!(
        "set -eu\n\
         NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n\
         cp /etc/crabka/config/broker-${{NODE_ID}}.toml /run/crabka/broker.toml\n\
         exec /usr/bin/crabka-broker \\\n  \
           --config-file=/run/crabka/broker.toml \\\n  \
           --broker-id=\"${{NODE_ID}}\"{metrics_flag}\n",
    )
}
```

`render_statefulset` reads `parent.spec.metrics_config.is_some()` and threads `metrics_enabled` through.

The existing `const MAIN_SCRIPT` is retained as a thin wrapper around `build_main_script(false)` so the no-metrics path produces a byte-identical string to slice 25:

```rust
// Asserted in a unit test below.
const MAIN_SCRIPT: &str = "…"; // slice-25 value, unchanged

#[test]
fn build_main_script_disabled_matches_slice_25_constant() {
    assert_eq!(build_main_script(false), MAIN_SCRIPT);
}
```

**Why a string-eq test rather than the const itself?** The slice-25 string is the upgrade-stability contract: existing clusters with `metrics_config = None` MUST get a byte-identical pod-template. The test fails loudly if a future edit silently breaks the contract.

---

## 4. Resource rendering (`controller/metrics.rs`)

New module. Three pure render functions + an apply helper.

### `render_metrics_service`

For `service_monitor` only. A headless `Service` named `<cluster>-broker-metrics`, selecting every broker pod of the cluster:

```rust
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

    let svc = serde_json::from_value(json!({
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
```

### `render_pod_monitor`

```rust
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
    let mut endpoint = serde_json::Map::new();
    endpoint.insert("port".into(), json!("metrics"));
    endpoint.insert("path".into(), json!("/metrics"));
    endpoint.insert(
        "interval".into(),
        json!(cfg.interval.as_deref().unwrap_or("30s")),
    );
    endpoint.insert(
        "scrapeTimeout".into(),
        json!(cfg.scrape_timeout.as_deref().unwrap_or("10s")),
    );

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
            "podMetricsEndpoints": [endpoint],
        }
    }))
}
```

### `render_service_monitor`

Same shape as `render_pod_monitor` but `kind: ServiceMonitor`, `spec.endpoints` (not `podMetricsEndpoints`), and the selector points at the metrics-only `Service` via its labels (the `<cluster>-broker-metrics` Service inherits `app.kubernetes.io/name=crabka-broker`, `instance=<name>`, so the same selector works).

### Apply helper — dynamic SSA

`monitoring.coreos.com/v1` is not in `k8s_openapi`. Apply via `kube::api::Api::all_with(client, &ApiResource)`:

```rust
async fn apply_dynamic(
    client: &kube::Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    plural: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), ReconcileError> {
    use kube::api::{Api, DynamicObject, Patch, PatchParams};
    use kube::core::{GroupVersionKind, ApiResource};

    let (group, version) = api_version.split_once('/').ok_or(
        ReconcileError::Malformed("apiVersion missing '/'".into()),
    )?;
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);

    let patch_params = PatchParams::apply(FIELD_MANAGER).force();
    let obj: DynamicObject = serde_json::from_value(body.clone())?;
    match api.patch(name, &patch_params, &Patch::Apply(&obj)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ae)) if ae.code == 404 && missing_crd(&ae) => {
            Err(ReconcileError::PrometheusOperatorCrdsMissing)
        }
        Err(e) => Err(e.into()),
    }
}

fn missing_crd(ae: &kube::core::ErrorResponse) -> bool {
    // 404 on a CRD apply means either "resource missing" (which we just
    // tried to create — impossible to hit) or "the API group/version
    // itself isn't served". Distinguish by the error message:
    // kube-apiserver returns "the server could not find the requested
    // resource" when the group is absent.
    ae.message.contains("could not find the requested resource")
        || ae.reason.eq_ignore_ascii_case("NotFound")
}
```

The `ReconcileError::PrometheusOperatorCrdsMissing` variant maps to a 30s requeue in the controller and a `MetricsReady=False reason=PrometheusOperatorCrdsMissing` condition. The user can install the prometheus-operator CRDs at any time without restarting the operator.

---

## 5. Reconcile wiring (`controller/kafka.rs`)

After the slice-25 listener reconcile and before the final `patch_status`:

```rust
let metrics_outcome = metrics::reconcile_metrics(&ctx, &obj, &name, &ns).await;
let metrics_condition = match &metrics_outcome {
    None => condition(
        "MetricsReady", "False", "Disabled",
        "spec.metricsConfig is not set",
    ),
    Some(Ok(())) => condition(
        "MetricsReady", "True", "Available",
        "metrics resources reconciled",
    ),
    Some(Err(ReconcileError::MetricsMutuallyExclusive)) => condition(
        "MetricsReady", "False", "MutuallyExclusive",
        "podMonitor and serviceMonitor are mutually exclusive",
    ),
    Some(Err(ReconcileError::PrometheusOperatorCrdsMissing)) => condition(
        "MetricsReady", "False", "PrometheusOperatorCrdsMissing",
        "monitoring.coreos.com/v1 is not served by the API server",
    ),
    Some(Err(e)) => return Err(e),  // propagate
};
```

`reconcile_metrics` returns `Option<Result<(), ReconcileError>>` — `None` for the unset case, `Some(Err)` for both transient (requeue) and permanent (validate) failures, `Some(Ok)` on success.

The requeue interval becomes the shorter of (current slice-25 backoff, 30s) when `MetricsReady=False reason=PrometheusOperatorCrdsMissing`.

### `reconcile_metrics` skeleton

```rust
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
    }

    Some(Ok(()))
}
```

### Orphan cleanup

When `metrics_config` transitions from `Some` to `None` the operator should garbage-collect the `PodMonitor` / `ServiceMonitor` / metrics `Service`. Owner-references handle delete-on-parent-delete, but a CR edit isn't a parent delete.

Approach: at the end of `reconcile_metrics`, if `cfg.pod_monitor.is_none()` we `DELETE` the `PodMonitor` by name (404-tolerant); same for `ServiceMonitor` + metrics Service when `cfg.service_monitor.is_none()`. The delete is unconditional inside the `Some(cfg)` arm, so toggling between modes also cleans up the abandoned variant.

When `metrics_config` itself becomes `None`, we delete both names. To avoid a one-time delete storm on slice-40 upgrade (clusters that never had `metrics_config` set), the delete is gated on a `crabka.io/metrics-rendered` annotation on the `Kafka` CR — set when we successfully apply, removed when we delete. Cold installs without this annotation skip the delete attempt entirely.

---

## 6. Helm chart RBAC + values

`charts/crabka-operator/templates/clusterrole.yaml` gains:

```yaml
  - apiGroups: ["monitoring.coreos.com"]
    resources: ["podmonitors", "servicemonitors"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

No `values.yaml` change. The chart-level `serviceMonitor.enabled` already controls a `ServiceMonitor` for the OPERATOR's own `/metrics` endpoint — that's unrelated to this slice's broker-pod scraping.

---

## 7. Testing

### Unit tests

In `crd/metrics.rs::tests`:

- `metrics_config_defaults_type_prometheus` — `{}` parses to `MetricsConfig { type: Prometheus, .. }`.
- `metrics_config_round_trips` — full struct with both monitor specs through JSON.
- `metrics_type_rejects_unknown` — `{"type":"jmxExporter"}` deserializes to an error citing `prometheus`.
- `kafka_spec_metrics_config_omitted_when_none` — serializes a `KafkaSpec` with `metrics_config = None`; assert the JSON does not contain `"metricsConfig"`.

In `controller/metrics.rs::tests` (renderer-pure, no kube client):

- `render_pod_monitor_minimal` — empty `PodMonitorSpec`; assert defaults `interval=30s`, `scrapeTimeout=10s`, correct selector + namespaceSelector.
- `render_pod_monitor_with_overrides_and_labels` — non-default interval/timeout + extra labels; assert wire shape.
- `render_service_monitor_minimal` — same shape as PodMonitor but `kind=ServiceMonitor`, `endpoints` instead of `podMetricsEndpoints`.
- `render_metrics_service_selector_matches_pods` — headless Service, named port `metrics`, selector pins `instance=<name>`.

In `controller/kafka_node_pool.rs::tests`:

- `build_main_script_disabled_matches_slice_25_constant` — already described above; the upgrade-stability contract.
- `build_main_script_enabled_appends_metrics_flag` — `metrics_enabled = true` → string ends with `--metrics-listen-addr=0.0.0.0:9404\n`.
- `render_statefulset_metrics_off_no_port` — pool whose parent has `metrics_config = None`; assert rendered container `ports` length == 1.
- `render_statefulset_metrics_on_adds_port` — `Some(MetricsConfig)`; assert second port `name=metrics, containerPort=9404`.

In `tests/reconcile_kafka.rs` (mock kube client via `tower::ServiceExt::oneshot`):

- `metrics_disabled_path_no_dynamic_apply` — `metrics_config=None`; mock state asserts zero `monitoring.coreos.com` requests.
- `pod_monitor_path_applies_one_resource` — `metrics_config.podMonitor = Some(default)`; mock state asserts one PATCH on `…/podmonitors/<name>-broker` with `Patch::Apply` and `FIELD_MANAGER=crabka-operator`.
- `service_monitor_path_applies_service_and_servicemonitor` — assert one Service PATCH (`<name>-broker-metrics`) AND one ServiceMonitor PATCH.
- `mutually_exclusive_validation_blocks_apply` — both set; mock state asserts zero `monitoring.coreos.com` requests AND status condition `MetricsReady=False reason=MutuallyExclusive`.
- `prom_operator_missing_sets_condition_and_requeues` — apply on `podmonitors` returns 404 with the not-found message; assert condition `MetricsReady=False reason=PrometheusOperatorCrdsMissing` and reconcile returns `Action::requeue(30s)` or shorter.

### E2E (kind)

Extend `.github/workflows/operator-e2e.yml`:

1. After `kind` cluster boots and before the existing slice-25 listener checks, install the prometheus-operator CRDs only:
   ```sh
   kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.79.2/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
   kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.79.2/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml
   ```
   (URL pinned to a release tag; renovate keeps it fresh.)
2. Patch the existing `demo` Kafka CR with `spec.metricsConfig.podMonitor.interval=15s`.
3. Wait for `kubectl wait Kafka/demo --for=condition=MetricsReady=True --timeout=60s`.
4. Assert `kubectl get podmonitor demo-broker -o jsonpath='{.spec.podMetricsEndpoints[0].port}' == "metrics"`.
5. Assert the pod has a `metrics` containerPort: `kubectl get pod demo-brokers-0 -o jsonpath='{.spec.containers[?(@.name=="broker")].ports[?(@.name=="metrics")].containerPort}' == "9404"`.
6. Curl `/metrics` from inside the cluster:
   ```sh
   kubectl run curl-metrics --rm -it --image=mirror.gcr.io/curlimages/curl --restart=Never -- \
     -s http://demo-brokers-0.demo-broker-headless.default.svc.cluster.local:9404/metrics \
     | grep -q '^# TYPE crabka_broker'
   ```

### Upgrade test (kind)

Reusing the slice-25 upgrade scaffold:

1. Install the slice-25 chart + a `Kafka` CR with `spec.config` set, NO `metricsConfig`.
2. Upgrade to the slice-40 chart.
3. Assert the broker pod's UID is unchanged (no roll): `[ "$uid_before" = "$uid_after" ]`.
4. Assert `MetricsReady=False reason=Disabled` is present.

The byte-identical pod-template unit test (`build_main_script_disabled_matches_slice_25_constant`) gives same-PR confidence; the upgrade test gives end-to-end proof.

---

## 8. File structure

```
crates/operator/src/crd/
├── metrics.rs                   # NEW — MetricsConfig + nested types + tests
├── kafka.rs                     # MODIFIED — KafkaSpec.metrics_config field + test
├── mod.rs                       # MODIFIED — re-export

crates/operator/src/controller/
├── metrics.rs                   # NEW — render_pod_monitor / service_monitor / metrics_service + apply_dynamic + reconcile_metrics
├── kafka.rs                     # MODIFIED — call reconcile_metrics, surface MetricsReady condition, orphan-cleanup annotation
├── kafka_node_pool.rs           # MODIFIED — metrics port + build_main_script(metrics_enabled)
├── common.rs                    # MODIFIED — new ReconcileError variants (MetricsMutuallyExclusive, PrometheusOperatorCrdsMissing, Malformed)
├── mod.rs                       # MODIFIED — pub(crate) mod metrics

crates/operator/tests/
├── reconcile_kafka.rs           # MODIFIED — five new metrics reconcile tests

charts/crabka-operator/templates/
├── clusterrole.yaml             # MODIFIED — monitoring.coreos.com rules

deploy/crds/
├── crabka.io_kafkas.yaml        # REGENERATED

.github/workflows/
├── operator-e2e.yml             # MODIFIED — install prom-op CRDs, assert PodMonitor + /metrics scrape
```

---

## 9. Conflict analysis (for parallel batching)

| File | Tasks touching it |
|---|---|
| `crd/metrics.rs` | T1 (create) |
| `crd/kafka.rs` | T1 (add field + test) |
| `crd/mod.rs` | T1 (re-export) |
| `controller/metrics.rs` | T2 (create renderer + reconcile_metrics) |
| `controller/kafka.rs` | T3 (wire into reconcile) |
| `controller/kafka_node_pool.rs` | T4 (pod-template changes) |
| `controller/common.rs` | T2 (one error variant) |
| `controller/mod.rs` | T2 (mod declaration) |
| `tests/reconcile_kafka.rs` | T3 (new test cases) |
| `charts/crabka-operator/templates/clusterrole.yaml` | T5 |
| `deploy/crds/crabka.io_kafkas.yaml` | T6 (regen) |
| `.github/workflows/operator-e2e.yml` | T7 |

Parallel batches:

- **Batch 1:** T1 (CRD types), T4 (pod-template), T5 (RBAC). Disjoint files; can run together.
- **Batch 2:** T2 (renderer + reconcile_metrics). Depends on T1's types.
- **Batch 3:** T3 (reconcile wiring + unit tests). Depends on T2.
- **Batch 4:** T6 (CRD regen), T7 (e2e workflow). Disjoint; can run together. Final manual verification.

Roughly: T1 ‖ T4 ‖ T5 → T2 → T3 → T6 ‖ T7.

---

## 10. Acceptance criteria

1. `cargo build -p crabka-operator` clean.
2. `cargo test -p crabka-operator` green (existing + ~12 new unit / reconcile tests).
3. `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. `cargo xtask gen-crds` produces no diff.
5. `helm lint charts/crabka-operator` passes.
6. operator-e2e job (kind): with prom-op CRDs installed, applying `Kafka` with `spec.metricsConfig.podMonitor` yields a `PodMonitor` of expected shape AND the broker pod serves `crabka_broker_*` metrics on `:9404`.
7. Slice-25-to-40 upgrade smoke: pre-existing `Kafka` without `metricsConfig` does NOT roll any broker pods on chart upgrade; `MetricsReady=False reason=Disabled` is set.

---

## 11. Open questions resolved

- **`Kafka.spec.metricsConfig` vs `KafkaNodePool.spec.metricsConfig`?** Cluster-level on `Kafka`. PodMonitor/ServiceMonitor are cluster-scoped objects selecting all broker pods; per-pool overrides have no use case yet.
- **Always-on metrics port vs conditional?** Conditional on `metrics_config.is_some()`. Keeps the slice-40 upgrade roll-free for clusters that don't opt in (mirrors slice 25's empty-`spec.listeners` zero-roll path).
- **Discover prometheus-operator CRDs at startup or on apply?** On apply. Lazy discovery means users can install the CRDs after the operator without bouncing the operator pod. The condition `PrometheusOperatorCrdsMissing` makes the state visible.
- **PodMonitor vs ServiceMonitor — pick one?** Both. ServiceMonitor needs a backing Service (which the operator generates); PodMonitor scrapes pods directly. Some prometheus-operator deployments select one but not the other via `Prometheus.spec.{pod,service}MonitorSelector`. Forcing one would lock a class of users out.
- **JSON dynamic apply vs adding `prometheus-operator-crd` dep?** Dynamic apply via `kube::api::DynamicObject`. The two resource types are tiny; an extra dependency for two structs isn't worth the build cost or the version-tracking burden.
- **Strimzi-shaped `metricsConfig.type=jmxPrometheusExporter` for migration tool friendliness?** No. Crabka has no JMX exporter — a `jmxPrometheusExporter` value cannot be honored, so accepting it would be misleading. The Phase-12 migration tool can map `jmxPrometheusExporter` → `prometheus` during import.
