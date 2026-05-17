# Slice 20c: Operator — Pod templates on `KafkaNodePool` — Design

**Status:** Approved 2026-05-17.

**Goal:** Add a `spec.template` surface to `KafkaNodePool` that lets operators control pod-level scheduling and metadata: affinity, tolerations, node selector, and pod-template labels/annotations. The field surface is intentionally minimal so the same shape can be reused by every later workload CRD (`KafkaConnect`, `KafkaMirrorMaker2`, `KafkaBridge`, etc.) without redesign.

---

## 1. Scope

### In

- `KafkaNodePoolSpec.template: Option<PodTemplate>`:
  - `template.metadata.labels` — extra labels applied to every pod (and to the `StatefulSet.spec.template.metadata.labels`). Operator-managed labels (`app.kubernetes.io/*`, `crabka.io/pool`) override on collision so users can't break the selector.
  - `template.metadata.annotations` — extra annotations on the pod template. No operator-managed annotations today, so user values win unconditionally.
  - `template.affinity` — passed through to `PodSpec.affinity`. K8s validates the structure.
  - `template.tolerations` — passed through to `PodSpec.tolerations`.
  - `template.nodeSelector` — passed through to `PodSpec.nodeSelector`.
- Renderer merges user-provided pod labels/annotations into the operator's labels and applies affinity/tolerations/nodeSelector under the StatefulSet pod template.
- StatefulSet selector remains a **stable subset** of the operator-managed labels (`app.kubernetes.io/name`, `app.kubernetes.io/instance`, `crabka.io/pool`). User labels never enter the selector — `StatefulSet.spec.selector` is immutable, so leaking user-controlled labels there would break edits.
- Schema validation: `nodeSelector` keys/values are unrestricted strings (k8s already validates). `affinity` / `tolerations` use `k8s_openapi` types so their JSON-schema embeddings come for free.
- E2E (kind): apply a `KafkaNodePool` with all three fields populated (a `tolerations` entry, a `nodeSelector` matching the kind node, a pod label + annotation) and assert via `kubectl get pod demo-brokers-0 -o jsonpath` that the values landed on the pod.

### Out (deferred)

| Concern | Slice |
|---|---|
| Multi-replica per pool | 20a |
| Controller-only / broker-only role separation | 20b |
| Container-level customization (extra env vars, sidecar containers, `securityContext` overrides) | future |
| Per-container resource overrides distinct from `spec.resources` | future |
| `Kafka.spec.template` (cluster-level fallback applied to all pools) | future |
| `template.podSecurityContext` override | future (the renderer's hardened defaults are non-negotiable in slice 20c) |
| `template.serviceAccount` | future (slice 36 KafkaUser may need this) |
| `template.imagePullSecrets` | future |
| Validation of conflicting selectors (e.g. operator label vs user label of the same key) | enforced silently by precedence today; a warning condition can come later |

### Constraints

- The slice 20 invariants stay: single-replica mixed-mode pools only. Pod templates apply uniformly to every replica.
- Operator-managed pod-template labels (the slice-20 set + `app.kubernetes.io/version`) always win over user labels.
- The selector labels (a strict subset of operator labels) are unchanged from slice 20.

---

## 2. CRD shape

Add to `crates/operator/src/crd/kafka_node_pool.rs`:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodTemplate {
    /// Extra labels / annotations on the pod template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataTemplate>,
    /// Forwarded to `PodSpec.affinity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<k8s_openapi::api::core::v1::Affinity>,
    /// Forwarded to `PodSpec.tolerations`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
    /// Forwarded to `PodSpec.nodeSelector`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataTemplate {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub labels: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub annotations: std::collections::BTreeMap<String, String>,
}
```

Add to `KafkaNodePoolSpec`:

```rust
/// Optional pod-level customization applied to every pod in this pool.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub template: Option<PodTemplate>,
```

The fields are flat (no `pod` indirection à la Strimzi) — Crabka's CRD doesn't multiplex StatefulSet vs PodSet vs DaemonSet, so the extra nesting buys nothing.

---

## 3. Renderer changes

`crates/operator/src/controller/kafka_node_pool.rs::render_statefulset`:

1. After building operator-managed `labels` (existing call to `common_labels`), merge user `template.metadata.labels` UNDERNEATH:

    ```rust
    let mut pod_labels = labels.clone();  // operator-managed
    if let Some(user_labels) = pool.spec.template.as_ref()
        .and_then(|t| t.metadata.as_ref())
        .map(|m| &m.labels)
    {
        for (k, v) in user_labels {
            // user labels never override operator labels
            pod_labels.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    ```

   Note: the StatefulSet's own `metadata.labels` (used for `kubectl get sts` UI) stays operator-only — user pod labels are pod-template-only.

2. Build pod annotations from `template.metadata.annotations` (operator has none today, so this is just a clone or empty map).

3. The rendered `template.spec` gains three optional fields when `template` is set:

    ```rust
    let mut pod_spec = json!({
        "securityContext": { ... existing hardened defaults ... },
        "initContainers": [init],
        "containers": [main],
        "volumes": [{ "name": "data", "emptyDir": {} }],
    });
    if let Some(tpl) = pool.spec.template.as_ref() {
        if let Some(affinity) = tpl.affinity.as_ref() {
            pod_spec["affinity"] = serde_json::to_value(affinity)?;
        }
        if !tpl.tolerations.is_empty() {
            pod_spec["tolerations"] = serde_json::to_value(&tpl.tolerations)?;
        }
        if let Some(ns) = tpl.node_selector.as_ref() {
            if !ns.is_empty() {
                pod_spec["nodeSelector"] = serde_json::to_value(ns)?;
            }
        }
    }
    ```

4. Pod-template metadata becomes:

    ```rust
    "template": {
        "metadata": {
            "labels": pod_labels,
            "annotations": pod_annotations,
        },
        "spec": pod_spec,
    },
    ```

   When `pod_annotations` is empty, the `annotations` key is omitted (server-side apply otherwise tracks ownership of an empty map and clobbers any field-manager-foreign annotations on subsequent applies).

The renderer must remain pure (no I/O); all merge logic is local to the function.

---

## 4. Testing

### Unit tests (in `controller/kafka_node_pool.rs::tests`)

- `render_statefulset_template_labels_merge_under_operator_labels` — user provides `{foo: bar, app.kubernetes.io/name: hijack}`; assert rendered pod-template labels contain `foo=bar` AND `app.kubernetes.io/name=crabka-broker` (operator wins).
- `render_statefulset_template_annotations_apply` — user provides `{custom-anno: v}`; assert rendered pod-template annotations contain `custom-anno=v`.
- `render_statefulset_affinity_passes_through` — construct a small `Affinity` (e.g., `podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution` with one term); assert it serializes into `spec.template.spec.affinity`.
- `render_statefulset_tolerations_passes_through` — user provides one toleration with key `dedicated`; assert it lands in `spec.template.spec.tolerations`.
- `render_statefulset_node_selector_passes_through` — user provides `{disktype: ssd}`; assert `spec.template.spec.nodeSelector` matches.
- `render_statefulset_no_template_no_extra_fields` — pool with `template = None`; assert the rendered pod spec has NO `affinity` / `tolerations` / `nodeSelector` keys.
- `pod_template_round_trips_through_json` (in CRD test module) — full struct including nested affinity rule.

### Integration tests

The slice-20 mocked-client tests don't need to assert on the renderer output (it's covered by unit tests). No changes required.

### E2E (kind)

The slice-20 workflow currently applies a `KafkaNodePool brokers` with `roles: [Controller, Broker]; replicas: 1; nodeIdStart: 0`. Extend it to include a `template`:

```yaml
spec:
  roles: [Controller, Broker]
  replicas: 1
  nodeIdStart: 0
  template:
    metadata:
      labels:
        team: platform
      annotations:
        crabka.io/test-anno: "yes"
    tolerations:
      - key: dedicated
        operator: Exists
        effect: NoSchedule
    nodeSelector:
      kubernetes.io/os: linux
```

(The `kubernetes.io/os: linux` selector matches every kind node automatically. The toleration is unneeded but proves the surface is wired — kind nodes have no taints to bypass, so the toleration is a no-op that doesn't cost anything.)

After the existing `Smoke — broker binary launched in pod` step, add:

```yaml
      - name: Smoke — pod template values applied
        run: |
          team=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.metadata.labels.team}')
          [ "$team" = "platform" ] || { echo "::error::team label missing, got '$team'"; exit 1; }

          anno=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.metadata.annotations.crabka\.io/test-anno}')
          [ "$anno" = "yes" ] || { echo "::error::annotation missing, got '$anno'"; exit 1; }

          ns=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.spec.nodeSelector.kubernetes\.io/os}')
          [ "$ns" = "linux" ] || { echo "::error::nodeSelector missing, got '$ns'"; exit 1; }

          tol_key=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.spec.tolerations[?(@.key=="dedicated")].key}')
          [ "$tol_key" = "dedicated" ] || { echo "::error::toleration missing"; exit 1; }
```

---

## 5. File structure

```
crates/operator/src/crd/
├── kafka_node_pool.rs            # MODIFIED — PodTemplate + MetadataTemplate types
crates/operator/src/controller/
├── kafka_node_pool.rs            # MODIFIED — renderer merges template fields
deploy/crds/
├── crabka.io_kafkanodepools.yaml # REGENERATED
.github/workflows/
├── operator-e2e.yml              # MODIFIED — apply with template, assert values
```

Implementation plan target: **4–5 tasks across 2 batches** (this slice is small).

- **Batch 1 (parallel):** T1 CRD types (PodTemplate + nested), T2 renderer changes (merge + pod_spec extension). T1 ships first because T2 needs the types.
- **Batch 2 (sequential):** T3 regenerate CRD YAML, T4 e2e workflow updates, T5 final verification.

In practice T1 → T2 → T3‖T4 → T5, because T2 imports T1's types.

---

## 6. Acceptance criteria

1. `cargo test -p crabka-operator` green (existing + new renderer / round-trip tests).
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` passes.
4. CRD regen is stable.
5. operator-e2e: `KafkaNodePool brokers` with `template.{metadata.labels, metadata.annotations, tolerations, nodeSelector}` lands on the pod and is observable via `kubectl get pod ... -o jsonpath`.

---

## 7. Open questions resolved

- **Strimzi-shape `spec.template.pod.{...}` vs flat `spec.template.{...}`?** Flat. Crabka's CRDs aren't multiplexing pod-template variants.
- **Should user labels be able to override operator labels?** No. Operator labels are load-bearing (selector, version pinning). User labels are pod-template-only and lose collisions.
- **Should `nodeSelector` validate keys against k8s rules?** No. The API server rejects invalid keys at admission; duplicating that check is busywork.
- **Should `template.metadata` flow onto the `StatefulSet`'s own metadata?** No — that would let users overwrite the operator's StatefulSet labels (which are referenced in dashboards). User labels are pod-template-only.
