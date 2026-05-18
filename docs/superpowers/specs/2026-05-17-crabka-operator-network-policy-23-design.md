# Slice 23: Operator — `Kafka.spec.networkPolicy` (NetworkPolicy generation) — Design

**Status:** Approved 2026-05-17.

**Goal:** Surface an opt-in `Kafka.spec.networkPolicy` field that lets a cluster operator restrict ingress to broker/controller pods via a generated `networking.k8s.io/v1 NetworkPolicy`. Per-listener peer allow-lists live on the existing `Listener` struct. The operator auto-allows its own admin-client traffic and (when configured) the metrics scrape port.

---

## 1. Scope

### In

- `KafkaSpec.network_policy: Option<NetworkPolicySpec>`. When `None`, no `NetworkPolicy` is generated (and any existing one is garbage-collected on transition). When `Some` (even `{}`), the operator generates one cluster-level `NetworkPolicy` owner-ref'd to the `Kafka`.
- `NetworkPolicySpec`: a marker struct with no required fields; future slices can add fields without a breaking change.
- `Listener.network_policy_peers: Option<Vec<NetworkPolicyPeer>>` — tri-state:
  - `None` → no per-listener peer restriction (allow-all on that port).
  - `Some(vec![])` → deny-all on that port (operator emits no listener-port rule; default-deny applies).
  - `Some(non_empty)` → only listed peers may reach that port.
- `NetworkPolicyPeer`: Crabka-defined subset of `networking.k8s.io/v1.NetworkPolicyPeer` carrying `pod_selector` + `namespace_selector` (both `Option<LabelSelector>`). `ipBlock` is omitted; a future slice can add it.
- Generated `NetworkPolicy` (one per cluster, named `<cluster>-broker-policy`):
  - `policyTypes: ["Ingress"]`.
  - `podSelector` matches every cluster pod (broker/controller/combined) via `app.kubernetes.io/name=crabka-broker` + `app.kubernetes.io/instance=<name>`.
  - Ingress rules, in stable order:
    1. **Inter-broker traffic** — pod-to-pod from the same selector on the inter-broker listener port. Always allowed.
    2. **Operator auto-allow** — pods labeled `app.kubernetes.io/name=crabka-operator` on every listener port. One rule per listener.
    3. **Per-listener peer rules** — one rule per listener whose `network_policy_peers` is not `Some([])`; allow-all when `None`, restricted when `Some(peers)`.
    4. **Metrics port (9404)** — allow-all rule when `spec.metricsConfig` is set.
- New status condition `NetworkPolicyReady` on `KafkaStatus.conditions`:
  - `Disabled` (status `False`) — `spec.networkPolicy` unset. Informational.
  - `Available` (status `True`) — `NetworkPolicy` reconciled successfully.
  - `Error` (status `False`) — apply failed; reconcile returns error and requeues.
- Orphan cleanup gated on the previous-reconcile status: when `spec.networkPolicy` is `None` AND `status.conditions` contains a `NetworkPolicyReady` entry with `reason=Available`, the operator deletes the policy. Otherwise the delete attempt is skipped. No annotation writes; the cached status is the source of truth.
- Helm chart RBAC additions for `networking.k8s.io/networkpolicies` (verbs `get,list,watch,create,update,patch,delete`).
- CRD YAML regenerated; `cargo xtask gen-crds` clean.
- Unit + reconcile tests; one operator-e2e job that installs Calico on the kind cluster (kindnet does not enforce `NetworkPolicy`) and asserts allow-vs-deny behaviour for a labeled-namespace peer.

### Out (deferred)

| Concern | Slice / why |
|---|---|
| `ipBlock` CIDR peers | future — add when external CIDR allow-lists become a need |
| Egress `NetworkPolicy` | future — broker egress is currently unrestricted; rare ask |
| Per-pool `networkPolicy` override | future — cluster-level is enough today |
| Operator-controlled `GlobalNetworkPolicy` / `CiliumNetworkPolicy` | out — standard `networking.k8s.io/v1` only |
| `spec.networkPolicy.metricsPeers` to scope :9404 ingress | future — allow-all is sufficient for the slice |
| Strimzi-compatibility for `Kafka.spec.kafka.networkPolicy*` field paths | out — Crabka uses flat `spec.networkPolicy`; Phase 12 migration tool can rewrite |
| Replication-quotas / SCRAM-listener ports | future — covered by their own slice when those listeners exist |

### Constraints inherited

- Crabka is greenfield: no `serde(default)` compat shims, no `V2` enum variants. `network_policy: Option<…>` defaulting to `None` is the only "compat" needed.
- Slice-21 config-hash drives rolling restart on `spec.config` / listener-intent change only. Setting `spec.networkPolicy` MUST NOT roll the broker pods — a `NetworkPolicy` is an apiserver-side firewall, not part of the pod template. No change to the slice-21 hash function.

---

## 2. CRD shape

### New module `crates/operator/src/crd/network_policy.rs`

```rust
//! Slice 23: `Kafka.spec.networkPolicy` — operator-side surface for
//! generating a cluster-level `networking.k8s.io/v1.NetworkPolicy`.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cluster-level opt-in for operator-managed NetworkPolicy generation.
/// Setting `Kafka.spec.networkPolicy` (including `{}`) enables generation.
/// `None` (field absent) disables generation and triggers orphan cleanup.
///
/// The struct intentionally carries no fields today — future slices can
/// add `metrics_peers`, `controller_peers`, etc. without a breaking schema
/// change.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicySpec {}

/// Subset of `networking.k8s.io/v1.NetworkPolicyPeer`. `ipBlock` is
/// intentionally omitted — a future slice can add it if external CIDR
/// allow-lists become a real need.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyPeer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_selector: Option<LabelSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,
}
```

### Add to `KafkaSpec` (`crd/kafka.rs`)

```rust
/// Slice 23: opt-in NetworkPolicy generation. When `None`, no
/// NetworkPolicy is generated. When `Some` (even `{}`), the operator
/// renders a cluster-level NetworkPolicy gating ingress to broker /
/// controller pods.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub network_policy: Option<crate::crd::NetworkPolicySpec>,
```

### Add to `Listener` (`crd/listener.rs`)

```rust
/// Slice 23: per-listener peer allow-list. Tri-state:
/// - `None` → no per-listener restriction (anyone reaching the cluster
///   network may connect on this port).
/// - `Some(vec![])` → deny-all on this listener port (no peer rule is
///   emitted; default-deny applies).
/// - `Some(non_empty)` → only listed peers may reach this port.
///
/// Only consulted when `Kafka.spec.networkPolicy` is set; otherwise inert.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub network_policy_peers: Option<Vec<crate::crd::NetworkPolicyPeer>>,
```

### Add to `crd/mod.rs`

```rust
pub mod network_policy;
pub use network_policy::{NetworkPolicyPeer, NetworkPolicySpec};
```

### Status reasons (`NetworkPolicyReady`)

| Condition | Reason | Resource rendered? |
|---|---|---|
| `network_policy` unset | `Disabled` | none — orphan cleanup runs if annotation present |
| Apply succeeds | `Available` | yes |
| Apply fails | `Error` | none for this reconcile; reconcile returns error and requeues |

`Disabled` is surfaced as `NetworkPolicyReady=False reason=Disabled` rather than condition absence, so `kubectl wait --for=condition=NetworkPolicyReady` distinguishes "not configured" from "configuring."

---

## 3. Rendering (`controller/network_policy.rs`)

New module. One pure render function + one reconcile function + one apply helper. Pattern mirrors `controller/metrics.rs` (slice 40), but the resource type is in `k8s_openapi`, so no dynamic-object plumbing is needed.

### `render_network_policy`

```rust
pub(crate) fn render_network_policy(
    owner: &Kafka,
    effective_listeners: &[Listener],
    inter_broker_port: i32,
    metrics_enabled: bool,
) -> Result<NetworkPolicy, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let ns = owner.meta().namespace.clone();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    // Pod selector: every cluster pod (broker/controller/combined) gets
    // app.kubernetes.io/name=crabka-broker and instance=<name>. A single
    // selector covers all node-pool roles.
    let mut pod_match = BTreeMap::new();
    pod_match.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    pod_match.insert("app.kubernetes.io/instance".into(), name.clone());
    let pod_selector = LabelSelector {
        match_labels: Some(pod_match.clone()),
        match_expressions: None,
    };

    // Operator allow-rule selector (one peer used across all listener
    // ingress rules).
    let mut operator_match = BTreeMap::new();
    operator_match.insert("app.kubernetes.io/name".into(), OPERATOR_LABEL.into());
    let operator_peer = K8sPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(operator_match),
            match_expressions: None,
        }),
        namespace_selector: None,
        ip_block: None,
    };

    // Inter-broker self-selector peer (broker pod ↔ broker pod).
    let self_peer = K8sPeer {
        pod_selector: Some(pod_selector.clone()),
        namespace_selector: None,
        ip_block: None,
    };

    let mut ingress: Vec<NetworkPolicyIngressRule> = Vec::new();

    // 1. Inter-broker rule.
    ingress.push(NetworkPolicyIngressRule {
        from: Some(vec![self_peer.clone()]),
        ports: Some(vec![NetworkPolicyPort {
            protocol: Some("TCP".into()),
            port: Some(IntOrString::Int(inter_broker_port)),
            end_port: None,
        }]),
    });

    // 2. Operator allow-rule per listener port. Stable ordering by
    //    `effective_listeners` (the caller passes them sorted upstream).
    for l in effective_listeners {
        ingress.push(NetworkPolicyIngressRule {
            from: Some(vec![operator_peer.clone()]),
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".into()),
                port: Some(IntOrString::Int(l.port)),
                end_port: None,
            }]),
        });
    }

    // 3. Per-listener peer rules. Tri-state on `network_policy_peers`:
    //    - None → emit allow-all rule (empty `from`).
    //    - Some([]) → skip (deny-all).
    //    - Some(peers) → convert + emit.
    for l in effective_listeners {
        let rule_from = match l.network_policy_peers.as_deref() {
            None => Some(vec![]),
            Some([]) => continue,
            Some(peers) => Some(peers.iter().map(to_k8s_peer).collect()),
        };
        ingress.push(NetworkPolicyIngressRule {
            from: rule_from,
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".into()),
                port: Some(IntOrString::Int(l.port)),
                end_port: None,
            }]),
        });
    }

    // 4. Metrics-port rule (allow-all when metricsConfig set).
    if metrics_enabled {
        ingress.push(NetworkPolicyIngressRule {
            from: Some(vec![]),
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".into()),
                port: Some(IntOrString::Int(METRICS_PORT)),
                end_port: None,
            }]),
        });
    }

    let np = NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(format!("{name}-broker-policy")),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(owner)?]),
            ..Default::default()
        },
        spec: Some(K8sNpSpec {
            pod_selector,
            policy_types: Some(vec!["Ingress".into()]),
            ingress: Some(ingress),
            egress: None,
        }),
    };
    Ok(np)
}

fn to_k8s_peer(p: &NetworkPolicyPeer) -> K8sPeer {
    K8sPeer {
        pod_selector: p.pod_selector.clone(),
        namespace_selector: p.namespace_selector.clone(),
        ip_block: None,
    }
}
```

**Why `from: Some(vec![])` for allow-all?** A `NetworkPolicyIngressRule` with `from: []` matches any source (k8s semantics: empty `from` ⇒ no peer restriction, only the `ports` list applies). This is distinct from omitting the rule entirely (default-deny).

**Why skip listeners with `Some([])` peers?** When the user opts into NetworkPolicy and explicitly sets `peers: []`, they want deny-all on that listener. We rely on the cluster `policyTypes: ["Ingress"]` default-deny semantics: any port without an allow-rule is dropped.

**Deny-all + operator allow-rule interaction.** A listener with `Some([])` still receives an operator allow-rule (the operator-allow loop runs unconditionally over every listener). This is intentional: "deny-all" means "no external clients reach this listener," not "the operator can't manage the cluster." Without this guarantee, a user could brick their own cluster by setting deny-all on every listener.

### `reconcile_network_policy`

```rust
pub(crate) async fn reconcile_network_policy(
    ctx: &Context,
    owner: &Kafka,
    name: &str,
    namespace: &str,
    effective_listeners: &[Listener],
    inter_broker_port: i32,
) -> Option<Result<(), ReconcileError>> {
    let np_api: Api<NetworkPolicy> = Api::namespaced(ctx.client.clone(), namespace);

    if owner.spec.network_policy.is_none() {
        // Orphan cleanup: only attempt when the previous reconcile
        // marked NetworkPolicyReady=Available in status. Cold disable
        // (no prior render) skips the delete attempt entirely. The
        // status is read from the watch cache; no extra GET.
        let was_rendered = owner
            .status
            .as_ref()
            .is_some_and(|s| s.conditions.iter().any(|c|
                c.type_ == "NetworkPolicyReady" && c.reason == "Available"
            ));
        if was_rendered {
            let _ = np_api
                .delete(&format!("{name}-broker-policy"), &DeleteParams::default())
                .await; // 404-tolerant
        }
        return None;
    }

    let metrics_enabled = owner.spec.metrics_config.is_some();
    let np = match render_network_policy(owner, effective_listeners, inter_broker_port, metrics_enabled) {
        Ok(np) => np,
        Err(e) => return Some(Err(e)),
    };
    let np_name = format!("{name}-broker-policy");
    if let Err(e) = apply_object(&np_api, &np_name, &np).await {
        return Some(Err(e));
    }
    Some(Ok(()))
}
```

The "was rendered" gate uses the cached `owner.status.conditions` rather than an annotation — no extra apiserver writes, no annotation lifecycle to track, and the watch cache already carries the status. The subsequent `patch_status` writes `NetworkPolicyReady=Disabled` so the next reconcile sees `was_rendered=false` and skips the delete (a single-shot delete on transition).

---

## 4. Reconcile wiring (`controller/kafka.rs`)

After the slice-40 `metrics_condition` block and before the final `patch_status`:

```rust
let inter_broker_port = effective_listeners
    .iter()
    .find(|l| l.name == inter_broker_name)
    .map(|l| l.port)
    .unwrap_or(BROKER_PORT);

let np_outcome = network_policy::reconcile_network_policy(
    &ctx, &obj, &name, &ns, &effective_listeners, inter_broker_port,
).await;
let np_condition = match &np_outcome {
    None => condition(
        "NetworkPolicyReady", "False", "Disabled",
        "spec.networkPolicy is not set",
    ),
    Some(Ok(())) => condition(
        "NetworkPolicyReady", "True", "Available",
        "network policy reconciled",
    ),
    Some(Err(_)) => condition(
        "NetworkPolicyReady", "False", "Error",
        "network policy reconcile failed",
    ),
};
```

Append `np_condition` to the existing `conditions` vector before `patch_status`. After the status patch, propagate any `Err(_)` from `np_outcome` so the controller requeues (same pattern as the metrics propagation in slice 40).

`inter_broker_port` resolution: the inter-broker listener's port is the listener whose `name` matches `inter_broker_name` (already computed earlier in the reconcile). When `spec.listeners` is empty, `inter_broker_name` is `"PLAIN"` and the synthesized listener has port `BROKER_PORT` (9092) — so the fallback to `BROKER_PORT` is defensive only.

### Tri-state semantics enforcement

`render_network_policy` is the single point of truth for the tri-state. The reconcile shouldn't filter listeners on the way in — pass the full `effective_listeners` slice and let the renderer's `match l.network_policy_peers.as_deref()` decide.

---

## 5. Helm chart RBAC

`charts/crabka-operator/templates/clusterrole.yaml` gains:

```yaml
  - apiGroups: ["networking.k8s.io"]
    resources: ["networkpolicies"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

No `values.yaml` change.

---

## 6. Testing

### Unit tests

In `crd/network_policy.rs::tests`:

- `network_policy_spec_empty_round_trips` — `{}` deserializes to `NetworkPolicySpec` default; re-serializes to `{}`.
- `peer_with_both_selectors_round_trips` — pod + namespace selector through JSON.
- `peer_omits_unset_selectors` — JSON does not contain `podSelector` / `namespaceSelector` when `None`.

In `crd/kafka.rs::tests`:

- `spec_omits_network_policy_when_none` — serializes a `KafkaSpec` with `network_policy=None`; assert JSON does not contain `"networkPolicy"`.
- `spec_carries_network_policy_when_set` — `{"networkPolicy":{}}` parses; `spec.network_policy` is `Some(default)`.

In `crd/listener.rs::tests`:

- `listener_with_empty_peers_round_trips` — `network_policy_peers=Some(vec![])` survives JSON round-trip.
- `listener_without_peers_omits_field` — `None` does not emit `networkPolicyPeers` key.
- `listener_with_named_peer_round_trips` — `Some(vec![NetworkPolicyPeer { pod_selector: Some(...), ... }])` survives round-trip.

In `controller/network_policy.rs::tests` (renderer-pure):

- `render_emits_inter_broker_rule` — even with zero listeners (synthesized default only), assert one rule with `from: self_peer` on `BROKER_PORT`.
- `render_emits_operator_allow_rule_per_listener` — given two listeners on ports 9092 + 9094, assert two operator-allow rules (one each port) with `from: [{pod_selector: {app.kubernetes.io/name: crabka-operator}}]`.
- `render_unset_peers_listener_emits_allow_all` — listener with `network_policy_peers=None` → rule with `from: vec![]` on that port.
- `render_empty_peers_listener_skips_port_rule` — listener with `network_policy_peers=Some(vec![])` → no per-listener rule for that port (only operator-allow + inter-broker if applicable).
- `render_non_empty_peers_listener_restricts` — listener with `Some(vec![peer])` → rule with `from: vec![converted_peer]`.
- `render_metrics_enabled_emits_metrics_port_rule` — `metrics_enabled=true` → rule for `:9404` with `from: vec![]`.
- `render_metrics_disabled_no_metrics_port_rule` — `metrics_enabled=false` → no `:9404` rule.
- `render_pod_selector_matches_pool_pods` — `spec.podSelector.matchLabels == {app.kubernetes.io/name: crabka-broker, app.kubernetes.io/instance: <name>}`.
- `render_policy_types_ingress_only` — `policy_types == ["Ingress"]`, `egress` is `None`.
- `render_owner_ref_set` — owner-ref to the parent Kafka, `controller=true`.
- `render_name_and_namespace` — `<cluster>-broker-policy` in the parent's namespace.

### Reconcile tests (`tests/reconcile_kafka.rs`)

Five new cases using the existing mock kube-client harness:

- `network_policy_disabled_path_no_apply` — `spec.network_policy=None`, no prior `NetworkPolicyReady=Available` condition; assert zero PATCH and zero DELETE on `…/networkpolicies/<name>-broker-policy`; status condition `NetworkPolicyReady=False reason=Disabled`.
- `network_policy_enabled_path_applies_one_resource` — `Some(NetworkPolicySpec::default())`; assert exactly one Patch::Apply on `…/networkpolicies/<name>-broker-policy` with `field_manager=crabka-operator, force=true`; status condition `NetworkPolicyReady=True reason=Available`.
- `network_policy_transition_deletes_on_disable` — fixture Kafka has `status.conditions[NetworkPolicyReady].reason=Available` + `spec.network_policy=None`; assert one DELETE call on `…/networkpolicies/<name>-broker-policy`.
- `cold_disabled_no_delete_attempt` — no prior `Available` condition, `spec.network_policy=None`; assert zero DELETE calls.
- `network_policy_listener_deny_all_skips_port_rule` — listener with `network_policy_peers=Some(vec![])`; the rendered NetworkPolicy body in the apply payload does NOT contain a per-listener rule for that listener's port (but DOES contain the operator-allow rule for it).

### E2E (`/.github/workflows/operator-e2e.yml`)

`kind`'s default CNI (`kindnet`) does **not** enforce `NetworkPolicy`. The e2e job needs a NetworkPolicy-enforcing CNI. Calico is the most common and ships a single-manifest install.

New e2e job (or extension of the existing one):

1. Boot kind with `disableDefaultCNI: true` in the kind config.
2. Install Calico via the upstream manifest pinned to a tag.
3. Wait for `kubectl get pods -n kube-system -l k8s-app=calico-node` to be Ready.
4. Apply a `Kafka` with `spec.networkPolicy: {}` and one internal listener restricted to a labeled namespace:
   ```yaml
   spec:
     listeners:
     - name: PLAIN
       port: 9092
       type: internal
       networkPolicyPeers:
       - namespaceSelector:
           matchLabels:
             role: clients
   ```
5. Wait `kubectl wait Kafka/demo --for=condition=NetworkPolicyReady=True --timeout=60s`.
6. Assert `kubectl get networkpolicy demo-broker-policy -o json` includes `policyTypes: ["Ingress"]` and the expected ingress-rule count.
7. Apply two test pods:
   - `client-allowed` in namespace `clients` (labeled `role: clients`).
   - `client-denied` in namespace `default` (unlabeled).
8. From `client-allowed`: `nc -w 5 demo-broker-headless 9092` → expect success (exit 0).
9. From `client-denied`: same `nc` → expect failure (non-zero exit, timeout).
10. Assert the operator's admin client (if used by slice-22 controlled-shutdown probes) still reaches brokers — implicitly true via the auto-injected operator allow-rule.

The Calico install adds ~30s to the e2e job. Acceptable.

### Upgrade test

The existing slice-25/15 upgrade scaffold already covers "no-op upgrade does not roll." Extend by:

1. Install previous release (no `networkPolicy` field).
2. Upgrade chart.
3. Assert pod UIDs unchanged.
4. Assert `NetworkPolicyReady=False reason=Disabled` is present.

---

## 7. File structure

```
crates/operator/src/crd/
├── network_policy.rs              # NEW — NetworkPolicySpec + NetworkPolicyPeer + tests
├── kafka.rs                       # MODIFIED — KafkaSpec.network_policy field + tests
├── listener.rs                    # MODIFIED — Listener.network_policy_peers field + tests
├── mod.rs                         # MODIFIED — re-export

crates/operator/src/controller/
├── network_policy.rs              # NEW — render + reconcile + tests
├── kafka.rs                       # MODIFIED — wire reconcile_network_policy, NetworkPolicyReady condition
├── mod.rs                         # MODIFIED — pub(crate) mod network_policy

crates/operator/tests/
├── reconcile_kafka.rs             # MODIFIED — 5 new reconcile tests

charts/crabka-operator/templates/
├── clusterrole.yaml               # MODIFIED — networking.k8s.io/networkpolicies rules

deploy/crds/
├── crabka.io_kafkas.yaml          # REGENERATED

.github/workflows/
├── operator-e2e.yml               # MODIFIED — Calico install + peer-restricted listener test
```

---

## 8. Conflict analysis (for parallel batching)

| File | Tasks touching it |
|---|---|
| `crd/network_policy.rs` | T1 (create) |
| `crd/listener.rs` | T1 (add field + tests) |
| `crd/kafka.rs` | T1 (add field + tests) |
| `crd/mod.rs` | T1 (re-export) |
| `controller/network_policy.rs` | T2 (create) |
| `controller/mod.rs` | T2 (mod declaration) |
| `controller/kafka.rs` | T3 (wire reconcile + status condition) |
| `tests/reconcile_kafka.rs` | T3 (5 new test cases) |
| `charts/.../clusterrole.yaml` | T4 (RBAC) |
| `deploy/crds/crabka.io_kafkas.yaml` | T5 (regen) |
| `.github/workflows/operator-e2e.yml` | T6 (Calico + peer test) |

Parallel batches:

- **Batch 1:** T1 (CRD types) ‖ T4 (RBAC). Disjoint files.
- **Batch 2:** T2 (renderer + reconcile_network_policy). Depends on T1.
- **Batch 3:** T3 (reconcile wiring + reconcile tests). Depends on T2.
- **Batch 4:** T5 (CRD regen) ‖ T6 (e2e). Disjoint. T5 depends on T1; T6 depends on T3's user-visible condition.

Roughly: T1 ‖ T4 → T2 → T3 → T5 ‖ T6.

---

## 9. Acceptance criteria

1. `cargo build -p crabka-operator` clean.
2. `cargo test -p crabka-operator` green (existing + ~16 new unit / reconcile tests).
3. `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. `cargo xtask gen-crds` produces no diff.
5. `helm lint charts/crabka-operator` passes.
6. operator-e2e (kind + Calico): a peer-restricted listener blocks unlabeled clients (nc timeout) and allows labeled clients (nc exit 0); `kubectl get networkpolicy demo-broker-policy` shows the rendered rules.
7. Upgrade smoke: pre-existing `Kafka` without `networkPolicy` does NOT roll any broker pods on chart upgrade; `NetworkPolicyReady=False reason=Disabled` is set.

---

## 10. Open questions resolved

- **`Kafka.spec.networkPolicy` vs `Kafka.spec.kafka.networkPolicy`?** Flat — consistent with slice-25's flat `Kafka.spec.listeners`. The Phase-12 migration tool rewrites Strimzi imports.
- **Opt-in vs opt-out?** Opt-in. Greenfield clusters get zero-impact upgrades; security-conscious operators enable explicitly. Matches slice 40's `metricsConfig` pattern.
- **Where do per-listener peers live: on `Listener` or under `spec.networkPolicy`?** On `Listener`. Peers + listener stay co-located and delete together; no name-reference indirection to validate.
- **Unset peers vs empty-list peers semantics?** Mirrors Strimzi: unset = allow-all (no restriction), empty list = deny-all.
- **Operator auto-allow rule?** Yes. Without it, a user who sets restrictive peers on every listener silently locks the operator out of slice-22 controlled-shutdown and any future admin-client work.
- **Metrics port (9404) handling?** Allow-all when `metricsConfig` is set. A future field `spec.networkPolicy.metricsPeers` can tighten if needed.
- **Pod selector scope: broker-only vs broker+controller?** Both. The shared `app.kubernetes.io/name=crabka-broker` label (set in `common_labels`) covers all node-pool roles with one selector; per-role split adds nothing today.
- **`ipBlock` peer support?** Out. The two `LabelSelector` fields are sufficient for the in-cluster case; external clients reach brokers via NodePort/LoadBalancer Services and `NetworkPolicy` only governs pod-network ingress anyway.
- **Egress NetworkPolicy?** Out. Crabka brokers' egress is currently unrestricted in all examples; restricting it is a separate slice.
- **Owner-ref delete cascade vs explicit cleanup?** Both. Owner-ref handles parent-delete (no leaks on `kubectl delete kafka demo`); the annotation-gated cleanup handles the `networkPolicy: Some(…) → None` transition where the parent is alive.
- **Slice 21 config-hash impact?** None. NetworkPolicy is apiserver-side firewall, not pod template. Setting / unsetting `spec.networkPolicy` must not roll the pods.
