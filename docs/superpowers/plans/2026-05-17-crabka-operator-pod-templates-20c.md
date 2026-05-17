# Crabka Operator Slice 20c — Pod templates on `KafkaNodePool`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Per CLAUDE.md, dispatch batches in parallel where file sets don't overlap. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add `spec.template` to `KafkaNodePool` so operators can control pod-level scheduling (`affinity`, `tolerations`, `nodeSelector`) and add labels/annotations to the pod template.

**Spec:** [`docs/superpowers/specs/2026-05-17-crabka-operator-pod-templates-20c-design.md`](../specs/2026-05-17-crabka-operator-pod-templates-20c-design.md).

---

## Batch overview

| Batch | Tasks | Files | Parallel? |
|---|---|---|---|
| 1 | T1 | `crd/kafka_node_pool.rs` | — |
| 2 | T2 | `controller/kafka_node_pool.rs` (depends on T1) | — |
| 3 | T3, T4 | CRD YAML regen; e2e workflow — disjoint | yes |
| 4 | T5 | verify only | — |

T1 → T2 are strictly sequential (T2 imports T1's types). Then T3 + T4 can run in parallel; T5 is the final verify.

---

## Task 1 — `PodTemplate` types on `KafkaNodePoolSpec`

**Files:**
- Modify: `crates/operator/src/crd/kafka_node_pool.rs`

- [ ] **Step 1: Add the new types**

Below the existing `NodeRole` enum:

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

- [ ] **Step 2: Append to `KafkaNodePoolSpec`**

Add the field (place it after `resources`):

```rust
    /// Optional pod-level customization applied to every pod in this pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<PodTemplate>,
```

- [ ] **Step 3: Update existing tests**

In `round_trips_through_json`, the constructed `KafkaNodePoolSpec` literal needs `template: None`. The `spec_defaults_replicas_to_one` test parses minimal JSON which doesn't have `template`; that's fine — `template` is optional with `default`.

Add a new test:

```rust
#[test]
fn pod_template_round_trips_through_json() {
    use k8s_openapi::api::core::v1::{Affinity, NodeAffinity, NodeSelector, NodeSelectorTerm, Toleration};

    let mut labels = std::collections::BTreeMap::new();
    labels.insert("team".into(), "platform".into());

    let template = PodTemplate {
        metadata: Some(MetadataTemplate {
            labels: labels.clone(),
            annotations: std::collections::BTreeMap::new(),
        }),
        affinity: Some(Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm::default()],
                }),
                preferred_during_scheduling_ignored_during_execution: None,
            }),
            ..Default::default()
        }),
        tolerations: vec![Toleration {
            key: Some("dedicated".into()),
            operator: Some("Exists".into()),
            effect: Some("NoSchedule".into()),
            ..Default::default()
        }],
        node_selector: Some({
            let mut m = std::collections::BTreeMap::new();
            m.insert("kubernetes.io/os".into(), "linux".into());
            m
        }),
    };
    let pool = KafkaNodePool::new("brokers", KafkaNodePoolSpec {
        roles: vec![NodeRole::Controller, NodeRole::Broker],
        replicas: 1,
        node_id_start: 0,
        image: None,
        resources: None,
        template: Some(template),
    });

    let json = serde_json::to_string(&pool).unwrap();
    assert!(json.contains("\"team\":\"platform\""), "labels: {json}");
    assert!(json.contains("\"dedicated\""), "tolerations: {json}");
    assert!(json.contains("\"nodeSelector\""), "node_selector: {json}");
    let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
    assert_eq!(back.spec, pool.spec);
}
```

- [ ] **Step 4: Verify**

```bash
cargo test -p crabka-operator --lib crd::kafka_node_pool
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: 4 tests pass; clippy clean.

---

## Task 2 — Renderer merges `template` fields

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

> **Sequencing:** must follow T1.

- [ ] **Step 1: Build merged pod labels**

In `render_statefulset`, after the existing `let labels = common_labels(...)`:

```rust
let mut pod_labels = labels.clone();
let mut pod_annotations: BTreeMap<String, String> = BTreeMap::new();
if let Some(meta) = pool.spec.template.as_ref().and_then(|t| t.metadata.as_ref()) {
    for (k, v) in &meta.labels {
        // user labels lose collisions with operator labels
        pod_labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    for (k, v) in &meta.annotations {
        pod_annotations.insert(k.clone(), v.clone());
    }
}
```

`labels` (operator-managed) is still used for the StatefulSet's own metadata.

- [ ] **Step 2: Build pod_spec with optional template fields**

Replace the existing inline `"template": { "metadata": {...}, "spec": {...} }` JSON block with:

```rust
let mut template_meta = json!({ "labels": pod_labels });
if !pod_annotations.is_empty() {
    template_meta["annotations"] = serde_json::to_value(&pod_annotations)?;
}

let mut pod_spec = json!({
    "securityContext": {
        "runAsNonRoot": true,
        "runAsUser": 65532,
        "fsGroup": 65532,
        "seccompProfile": { "type": "RuntimeDefault" }
    },
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

let sts: StatefulSet = serde_json::from_value(json!({
    "metadata": {
        "name": sts_name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [owner_ref::<KafkaNodePool>(pool)?],
    },
    "spec": {
        "serviceName": service_name,
        "replicas": pool.spec.replicas,
        "podManagementPolicy": "Parallel",
        "selector": { "matchLabels": selector },
        "template": {
            "metadata": template_meta,
            "spec": pod_spec,
        }
    }
}))?;
```

- [ ] **Step 3: Tests**

Add to the existing `#[cfg(test)] mod tests`:

```rust
fn pool_with_template(template: PodTemplate) -> KafkaNodePool {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.template = Some(template);
    pool
}

#[test]
fn render_statefulset_template_labels_merge_under_operator_labels() {
    let mut user_labels = BTreeMap::new();
    user_labels.insert("team".into(), "platform".into());
    user_labels.insert("app.kubernetes.io/name".into(), "hijack".into());

    let pool = pool_with_template(PodTemplate {
        metadata: Some(MetadataTemplate { labels: user_labels, annotations: BTreeMap::new() }),
        ..Default::default()
    });
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let pod_labels = sts.spec.unwrap().template.metadata.unwrap().labels.unwrap();
    assert_eq!(pod_labels.get("team").map(String::as_str), Some("platform"));
    // operator-managed name MUST win
    assert_eq!(pod_labels.get("app.kubernetes.io/name").map(String::as_str), Some(APP_LABEL));
}

#[test]
fn render_statefulset_template_annotations_apply() {
    let mut annos = BTreeMap::new();
    annos.insert("crabka.io/test-anno".into(), "yes".into());
    let pool = pool_with_template(PodTemplate {
        metadata: Some(MetadataTemplate { labels: BTreeMap::new(), annotations: annos }),
        ..Default::default()
    });
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let anno = sts.spec.unwrap().template.metadata.unwrap().annotations.unwrap();
    assert_eq!(anno.get("crabka.io/test-anno").map(String::as_str), Some("yes"));
}

#[test]
fn render_statefulset_affinity_passes_through() {
    use k8s_openapi::api::core::v1::{Affinity, NodeAffinity, NodeSelector, NodeSelectorTerm};
    let affinity = Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm::default()],
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        ..Default::default()
    };
    let pool = pool_with_template(PodTemplate { affinity: Some(affinity.clone()), ..Default::default() });
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let rendered = sts.spec.unwrap().template.spec.unwrap().affinity;
    assert_eq!(rendered, Some(affinity));
}

#[test]
fn render_statefulset_tolerations_passes_through() {
    use k8s_openapi::api::core::v1::Toleration;
    let tol = Toleration {
        key: Some("dedicated".into()),
        operator: Some("Exists".into()),
        effect: Some("NoSchedule".into()),
        ..Default::default()
    };
    let pool = pool_with_template(PodTemplate { tolerations: vec![tol.clone()], ..Default::default() });
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let tols = sts.spec.unwrap().template.spec.unwrap().tolerations.unwrap();
    assert_eq!(tols, vec![tol]);
}

#[test]
fn render_statefulset_node_selector_passes_through() {
    let mut ns = BTreeMap::new();
    ns.insert("disktype".into(), "ssd".into());
    let pool = pool_with_template(PodTemplate { node_selector: Some(ns.clone()), ..Default::default() });
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let rendered = sts.spec.unwrap().template.spec.unwrap().node_selector.unwrap();
    assert_eq!(rendered.get("disktype").map(String::as_str), Some("ssd"));
}

#[test]
fn render_statefulset_no_template_no_extra_fields() {
    let pool = pool_fixture("brokers", "demo", 1);
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let spec = sts.spec.unwrap().template.spec.unwrap();
    assert!(spec.affinity.is_none());
    assert!(spec.tolerations.is_none() || spec.tolerations.as_ref().unwrap().is_empty());
    assert!(spec.node_selector.is_none() || spec.node_selector.as_ref().unwrap().is_empty());
}
```

The exact field shapes (`spec.template.metadata.labels: Option<BTreeMap<...>>`) depend on the `k8s_openapi` version — adjust unwraps as needed.

- [ ] **Step 4: Verify**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator --lib controller::kafka_node_pool::tests
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: 8 existing + 6 new tests pass; clippy clean.

---

## Task 3 — Regenerate CRD YAML

**Files:**
- Modify: `deploy/crds/crabka.io_kafkanodepools.yaml`

- [ ] **Step 1: Run gen-crds**

```bash
cargo run -p crabka-operator -- gen-crds deploy/crds
```

- [ ] **Step 2: Diff sanity**

```bash
git diff deploy/crds/crabka.io_kafkanodepools.yaml | head -100
```

Expected: new `template` property under `spec` with nested `metadata`, `affinity`, `tolerations`, `nodeSelector` schemas.

---

## Task 4 — operator-e2e: apply with template, assert values

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Extend the apply manifest**

In the existing "Apply Kafka + KafkaNodePool" step, the `KafkaNodePool brokers` section becomes:

```yaml
          apiVersion: crabka.io/v1alpha1
          kind: KafkaNodePool
          metadata:
            name: brokers
            namespace: default
            labels:
              crabka.io/cluster: demo
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

- [ ] **Step 2: Insert smoke step**

After the existing `Smoke — broker binary launched in pod` step, add:

```yaml
      - name: Smoke — pod template values applied
        run: |
          set -e
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

## Task 5 — Final verification

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `helm lint charts/crabka-operator`
- [ ] `cargo run -p crabka-operator -- gen-crds deploy/crds` is stable
- [ ] Commit, push, PR.

Commit title: `Slice 20c: Operator — pod templates on KafkaNodePool`.
