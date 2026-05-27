# Crabka Operator Slice 21 — Rolling restart on config drift

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Per CLAUDE.md, dispatch batches in parallel where file sets don't overlap. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add `Kafka.spec.config` (opaque broker properties), serialize into the broker ConfigMap, propagate a sha256 of the content via pool labels into a pod-template annotation, and expose a `Rolling` status condition. Editing `spec.config` rolls the broker pod naturally via K8s.

**Spec:** [`docs/superpowers/specs/2026-05-17-crabka-operator-rolling-restart-21-design.md`](../specs/2026-05-17-crabka-operator-rolling-restart-21-design.md).

---

## Batch overview

| Batch | Tasks | Files | Parallel? |
|---|---|---|---|
| 1 | T1, T2 | `crd/kafka.rs`; `controller/common.rs` — disjoint | yes |
| 2 | T3, T4 | `controller/kafka.rs`; `controller/kafka_node_pool.rs` — disjoint | yes |
| 3 | T5 | CRD regen + e2e workflow + verify | — |

---

## Task 1 — `Kafka.spec.config` field

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`

- [ ] **Step 1: Add the field**

Append to `KafkaSpec`:

```rust
/// Opaque broker properties (`server.properties`-style key/value
/// pairs). Serialized into the broker `ConfigMap`; changes propagate
/// through a content hash that triggers a rolling restart.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub config: Option<std::collections::BTreeMap<String, String>>,
```

- [ ] **Step 2: Update tests**

Existing `round_trips_through_json` and `spec_only_carries_kafka_version` need a `config: None` field on their `KafkaSpec` literals. Add a new test:

```rust
#[test]
fn spec_carries_config() {
    let json = r#"{"kafkaVersion":"0.1.1","config":{"log.retention.hours":"24"}}"#;
    let spec: KafkaSpec = serde_json::from_str(json).unwrap();
    let cfg = spec.config.expect("config present");
    assert_eq!(cfg.get("log.retention.hours").map(String::as_str), Some("24"));
}
```

- [ ] **Step 3: Verify**

```bash
cargo test -p crabka-operator --lib crd::kafka::
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: 4 tests pass; clippy clean. The crate may fail to compile elsewhere (reconciler reads `.config` once Task 3 lands) — that's fine if `cargo check --lib` errors. Use `cargo check -p crabka-operator --lib` to confirm only the controller modules complain.

---

## Task 2 — `config_hash` helper

**Files:**
- Modify: `crates/operator/src/controller/common.rs`
- Possibly: `crates/operator/Cargo.toml` if `sha2` isn't already pulled in.

- [ ] **Step 1: Verify the `sha2` dep**

```bash
grep -rE 'sha2 ?=' crates/*/Cargo.toml Cargo.toml | head -3
```

`sha2` is a workspace dep already (used by `crabka-security`). Add it as a `crabka-operator` dep if missing:

```toml
[dependencies]
sha2 = { workspace = true }
```

(If the workspace doesn't define it at workspace-level yet, look at `crabka-security`'s Cargo.toml; the same version pin works.)

- [ ] **Step 2: Helper**

In `crates/operator/src/controller/common.rs`, append:

```rust
/// SHA-256 hex digest of the given content. Used by slice 21 to detect
/// `Kafka.spec.config` changes that the K8s StatefulSet controller can't
/// see directly.
#[must_use]
pub fn config_hash(content: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(content.as_bytes());
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod config_hash_tests {
    use super::*;

    #[test]
    fn config_hash_is_sha256_hex() {
        // known sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        let h = config_hash("hello");
        assert_eq!(h, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
    }

    #[test]
    fn config_hash_empty_string() {
        let h = config_hash("");
        assert_eq!(h, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }
}
```

- [ ] **Step 3: Verify**

```bash
cargo test -p crabka-operator --lib controller::common::
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: 2 new tests pass.

---

## Task 3 — Kafka reconciler: serialize, hash, label patch, Rolling cond

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`

> **Sequencing:** depends on T1 (`spec.config` field) and T2 (`config_hash` helper).

- [ ] **Step 1: Serialize `spec.config` into broker.properties**

Add helper:

```rust
/// Serialize `spec.config` into a deterministic `broker.properties`
/// string (one `key=value` line per entry, BTreeMap iteration = sorted).
/// Returns `""` when `config` is None or empty.
fn serialize_broker_properties(spec: &crate::crd::KafkaSpec) -> String {
    let Some(cfg) = spec.config.as_ref() else { return String::new(); };
    let mut out = String::new();
    for (k, v) in cfg {
        out.push_str(k);
        out.push('=');
        out.push_str(v);
        out.push('\n');
    }
    out
}
```

- [ ] **Step 2: Plumb into `render_configmap`**

Open `crates/operator/src/controller/common.rs`. `render_configmap` currently builds a fixed `data: { broker.env: ... }`. Extend it to also include `broker.properties` when the spec has a non-empty config:

```rust
pub(crate) fn render_configmap(owner: &Kafka) -> Result<ConfigMap, ReconcileError> {
    let mut data: BTreeMap<String, String> = BTreeMap::new();
    data.insert("broker.env".into(), "CRABKA_LISTEN_ADDR=0.0.0.0:9092\n".into());
    let broker_props = crate::controller::kafka::serialize_broker_properties(&owner.spec);
    if !broker_props.is_empty() {
        data.insert("broker.properties".into(), broker_props);
    }
    // ... existing body that constructs the ConfigMap ...
}
```

Note: this means `controller::common` depends on `controller::kafka::serialize_broker_properties`. To avoid the circular dep, move the helper to `controller::common` instead:

```rust
// in controller/common.rs
pub(crate) fn serialize_broker_properties(spec: &crate::crd::KafkaSpec) -> String { ... }
```

and call it from `render_configmap` and from `controller::kafka::reconcile`.

- [ ] **Step 3: Compute hash in `reconcile`**

In `controller::kafka::reconcile`, after the existing `apply_object` for the ConfigMap:

```rust
let broker_props = common::serialize_broker_properties(&obj.spec);
let cfg_hash = common::config_hash(&broker_props);
```

- [ ] **Step 4: Extend `adopt_pools` to include the hash label**

Edit the SSA patch body in `adopt_pools` (and update its signature to accept the hash):

```rust
async fn adopt_pools<'a>(
    pool_api: &Api<KafkaNodePool>,
    parent: &Kafka,
    pools: impl IntoIterator<Item = &'a KafkaNodePool>,
    config_hash: &str,
) -> Result<(), ReconcileError> {
    let owner = owner_ref::<Kafka>(parent)?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    let patch_body = json!({
        "apiVersion": KafkaNodePool::api_version(&()),
        "kind": KafkaNodePool::kind(&()),
        "metadata": {
            "ownerReferences": [owner],
            "labels": { "crabka.io/config-hash": config_hash },
        }
    });
    for pool in pools {
        let pool_name = pool.name_any();
        pool_api.patch(&pool_name, &params, &Patch::Apply(&patch_body)).await?;
    }
    Ok(())
}
```

Update the call site in `reconcile` to pass `&cfg_hash`.

- [ ] **Step 5: Rolling condition**

Add a helper:

```rust
pub(crate) fn rolling_condition_from_rollup(rollup: &ClusterRollup) -> (bool, &'static str, String) {
    if rollup.pool_count > 0 && rollup.ready_replicas < rollup.replicas {
        (true, "RollingUpdate", format!("{}/{} brokers ready (roll in progress)",
            rollup.ready_replicas, rollup.replicas))
    } else {
        (false, "Stable", format!("all brokers on current revision"))
    }
}
```

In `reconcile`, after computing the existing `Ready` condition, append a `Rolling` condition:

```rust
let (rolling, rolling_reason, rolling_message) = rolling_condition_from_rollup(&rollup);
let conditions = vec![
    condition("Ready", if ready { "True" } else { "False" }, reason, &message),
    condition("Rolling", if rolling { "True" } else { "False" }, rolling_reason, &rolling_message),
];
let status = KafkaStatus { conditions, replicas: Some(rollup.replicas), ready_replicas: Some(rollup.ready_replicas) };
```

- [ ] **Step 6: Unit tests**

```rust
#[test]
fn serialize_broker_properties_sorted() {
    let mut cfg = BTreeMap::new();
    cfg.insert("log.retention.hours".into(), "24".into());
    cfg.insert("num.partitions".into(), "3".into());
    let spec = KafkaSpec { kafka_version: "0.1.1".into(), config: Some(cfg) };
    let s = serialize_broker_properties(&spec);
    // Keys sort alphabetically: log.retention.hours < num.partitions
    assert_eq!(s, "log.retention.hours=24\nnum.partitions=3\n");
}

#[test]
fn serialize_broker_properties_none_is_empty_string() {
    let spec = KafkaSpec { kafka_version: "0.1.1".into(), config: None };
    assert_eq!(serialize_broker_properties(&spec), "");
}

#[test]
fn rolling_condition_when_pool_partial() {
    let r = ClusterRollup { replicas: 3, ready_replicas: 1, pool_count: 1 };
    let (rolling, reason, _) = rolling_condition_from_rollup(&r);
    assert!(rolling);
    assert_eq!(reason, "RollingUpdate");
}

#[test]
fn rolling_condition_when_pool_stable() {
    let r = ClusterRollup { replicas: 1, ready_replicas: 1, pool_count: 1 };
    let (rolling, reason, _) = rolling_condition_from_rollup(&r);
    assert!(!rolling);
    assert_eq!(reason, "Stable");
}
```

- [ ] **Step 7: Integration tests in `tests/reconcile_kafka.rs`**

The slice-20c happy-path rule list adds a CONFIGMAP patch body now containing `broker.properties` when spec.config is set, and the pool adopt PATCH body now also contains `metadata.labels.crabka.io/config-hash`. Adjust existing test bodies that pin on exact body content; add two new tests:

```rust
#[tokio::test]
async fn kafka_writes_broker_properties_data_when_config_set() {
    // Set up fixture with spec.config = { log.retention.hours: "24" }.
    // Drive reconcile, capture the CM PATCH.
    let cm_patch_body = ... ;
    assert!(cm_patch_body["data"]["broker.properties"]
        .as_str().unwrap().contains("log.retention.hours=24"));
}

#[tokio::test]
async fn kafka_patches_pool_label_with_config_hash() {
    // Same fixture, capture the pool adopt PATCH body.
    // sha256("log.retention.hours=24\n") = <known value>
    let pool_patch_body = ... ;
    let hash = pool_patch_body["metadata"]["labels"]["crabka.io/config-hash"]
        .as_str().expect("hash");
    assert_eq!(hash, "<sha256 of 'log.retention.hours=24\\n'>");
}

#[tokio::test]
async fn kafka_status_includes_rolling_condition_stable() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    // ... drive reconcile, capture status PATCH ...
    let status_body = ...;
    let conds = status_body["status"]["conditions"].as_array().unwrap();
    let rolling = conds.iter().find(|c| c["type"] == "Rolling").unwrap();
    assert_eq!(rolling["status"], "False");
    assert_eq!(rolling["reason"], "Stable");
}
```

Compute the expected hash dynamically inside the test (use `common::config_hash` directly via `crabka_operator::controller::common::config_hash`).

- [ ] **Step 8: Verify**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: ~4 new unit tests + 3 new integration tests pass; existing 42 still pass; clippy clean.

---

## Task 4 — Pool reconciler: propagate annotation from label

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

> **Sequencing:** can run in parallel with T3 — pool reconciler only reads `pool.metadata.labels["crabka.io/config-hash"]`, doesn't depend on the Kafka reconciler's internals. Just on T1's `Kafka.spec.config` type for the test fixture.

- [ ] **Step 1: Read the label in `render_statefulset`**

In the existing `pod_annotations` build-up block:

```rust
if let Some(hash) = pool
    .metadata
    .labels
    .as_ref()
    .and_then(|l| l.get("crabka.io/config-hash"))
{
    pod_annotations.insert("crabka.io/config-hash".into(), hash.clone());
}
```

Place after the existing user-annotation merge (so user annotations land first; operator-owned `crabka.io/config-hash` overwrites if a user set the same key).

- [ ] **Step 2: Tests**

```rust
#[test]
fn render_statefulset_propagates_config_hash_from_label() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.metadata.labels.get_or_insert_with(BTreeMap::new)
        .insert("crabka.io/config-hash".into(), "abc123".into());
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let anno = sts.spec.unwrap().template.metadata.unwrap().annotations.unwrap();
    assert_eq!(anno.get("crabka.io/config-hash").map(String::as_str), Some("abc123"));
}

#[test]
fn render_statefulset_no_config_hash_when_label_absent() {
    let pool = pool_fixture("brokers", "demo", 1);
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let anno = sts.spec.unwrap().template.metadata.unwrap().annotations;
    // Annotation map may be None or just lack our key.
    if let Some(map) = anno {
        assert!(!map.contains_key("crabka.io/config-hash"));
    }
}
```

- [ ] **Step 3: Verify**

```bash
cargo test -p crabka-operator --lib controller::kafka_node_pool::tests
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: 14 existing + 2 new tests pass.

---

## Task 5 — CRD regen + e2e workflow + final verify

**Files:**
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated)
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Regenerate CRDs**

```bash
cargo run -p crabka-operator -- gen-crds deploy/crds
git diff deploy/crds/crabka.io_kafkas.yaml | head -20
```

Expected diff: new `config` property under `spec` with `additionalProperties: { type: string }`.

- [ ] **Step 2: Update operator-e2e**

In the existing "Apply Kafka + KafkaNodePool" step, set initial spec.config on the Kafka:

```yaml
          apiVersion: crabka.io/v1alpha1
          kind: Kafka
          metadata: { name: demo, namespace: default }
          spec:
            kafkaVersion: "0.1.1"
            config:
              log.retention.hours: "24"
```

After the existing `Smoke — pod template values applied` step (and before `Garbage-collection on Kafka delete`), insert:

```yaml
      - name: Smoke — config change rolls broker pod
        run: |
          set -e
          initial_uid=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.metadata.uid}')
          initial_rev=$(kubectl get sts demo-brokers -n default -o jsonpath='{.status.currentRevision}')
          echo "before: uid=$initial_uid rev=$initial_rev"

          kubectl patch kafka demo -n default --type=merge \
            -p '{"spec":{"config":{"log.retention.hours":"48"}}}'

          for i in $(seq 1 45); do
            current_uid=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
            echo "attempt $i: uid=$current_uid"
            if [ -n "$current_uid" ] && [ "$current_uid" != "$initial_uid" ]; then
              break
            fi
            sleep 2
          done

          # Wait for the new pod to reach Ready.
          kubectl wait --for=condition=Ready -n default pod/demo-brokers-0 --timeout=120s

          final_rev=$(kubectl get sts demo-brokers -n default -o jsonpath='{.status.currentRevision}')
          [ "$final_rev" != "$initial_rev" ] || { echo "::error::sts revision didn't change ($initial_rev -> $final_rev)"; exit 1; }
          echo "rolled: $initial_rev -> $final_rev"
```

The GC step and diagnostics block stay unchanged.

- [ ] **Step 3: Final verify**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
helm lint charts/crabka-operator
cargo run -p crabka-operator -- gen-crds deploy/crds && git diff --exit-code deploy/crds/
```

All green → commit, push, PR.

Commit title: `Slice 21: Operator — rolling restart on config drift`.
