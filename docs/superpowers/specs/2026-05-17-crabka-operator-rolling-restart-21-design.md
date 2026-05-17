# Slice 21: Operator — Rolling restart on config drift — Design

**Status:** Approved 2026-05-17.

**Goal:** Give operators a knob to mutate broker config and have the operator roll the StatefulSet automatically. Introduce `Kafka.spec.config` (an opaque `key=value` map), serialize it into the broker `ConfigMap`, propagate a sha256 of that content via pool labels into a pod-template annotation, and expose a `Rolling` status condition that mirrors `StatefulSet.status.{currentRevision,updateRevision}`. K8s does one-pod-at-a-time naturally; slice 21 wires the trigger and observability.

---

## 1. Scope

### In

- New optional `Kafka.spec.config: Option<BTreeMap<String, String>>`. Opaque broker properties (`server.properties`-style key/value pairs). The operator serializes the map into the existing `<name>-broker-config` `ConfigMap` under a new data key `broker.properties` (one `key=value` line per entry, sorted by key for determinism). Empty / unset map → key omitted.
- Operator computes a `crabka.io/config-hash` = sha256 (lowercase hex) of the rendered `broker.properties` content. When the spec is empty, the hash is the sha256 of the empty string. Hash recomputed on every `Kafka` reconcile.
- Hash propagates to broker pods via a **pool label**, NOT a direct StatefulSet annotation:
  - `Kafka` reconciler patches each matched pool's `metadata.labels["crabka.io/config-hash"]` (alongside the existing owner-ref injection).
  - `KafkaNodePool` reconciler reads that label and stamps it into the `StatefulSet.spec.template.metadata.annotations["crabka.io/config-hash"]`.
  - K8s rolls pods naturally when the template annotation changes (one-at-a-time, `partition: 0`, default `RollingUpdate`).
- `Kafka.status.conditions` gains a second condition `Rolling`:
  - `Rolling=True, reason=RollingUpdate, message="rolling brokers in pool <name>"` when any pool's `StatefulSet.status.currentRevision != updateRevision` (or `updatedReplicas < replicas`).
  - `Rolling=False, reason=Stable, message="all brokers on revision <rev>"` otherwise.
- E2E (kind): apply `Kafka demo` with `spec.config: {log.retention.hours: "24"}`. Wait `Ready=True`. Patch the Kafka to set `log.retention.hours: "48"`. Observe `pod demo-brokers-0` is replaced (UID changes; new revision applied).

### Out (deferred)

| Concern | Slice |
|---|---|
| Broker actually consumes `broker.properties` from the ConfigMap | future broker-side slice |
| Schema validation of config keys (allowlist, type-checking) | future |
| Plugin / log-appender / `kafka.logging` config | 41 |
| ISR-aware roll ordering across replicas | 20a / 21b once multi-replica lands |
| Broker-side `ControlledShutdown` on SIGTERM | separate (broker-side) |
| Per-pool overrides (`KafkaNodePool.spec.config`) | future — slice 21c if/when needed |
| Forced restart annotation (`kubectl annotate kafka demo crabka.io/restart=now`) | future |
| Roll progress in `Kafka.status` beyond the boolean condition | future |
| `maxUnavailable` tuning on `StatefulSet.updateStrategy.rollingUpdate` | future (default of 1 is correct for slice 20's single-replica) |

### Constraints

- Slice 20 invariants stay: single-replica mixed-mode pools, `KafkaNodePool` is the only StatefulSet owner.
- The hash is purely an opaque change-detector. The operator does NOT validate `spec.config` contents — the broker is the source of truth when it eventually consumes them.
- `crabka.io/config-hash` is owned by the operator at every level (pool label + pod-template annotation). User edits to those keys are silently overwritten on the next reconcile.

---

## 2. CRD shape

`crates/operator/src/crd/kafka.rs` — add to `KafkaSpec`:

```rust
/// Opaque broker properties (`server.properties`-style key/value
/// pairs). The operator serializes these into the broker `ConfigMap`
/// and propagates a hash so changes trigger a rolling restart.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub config: Option<std::collections::BTreeMap<String, String>>,
```

No new structs needed — `BTreeMap<String, String>` is `JsonSchema`-friendly.

`KafkaStatus` — extend the conditions list. The existing `Vec<KafkaCondition>` accommodates `Ready` + `Rolling` without schema changes. The operator must preserve both conditions when patching status (no clobbering between reconciles).

---

## 3. ConfigMap shape

`render_configmap` now emits both keys:

```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: demo-broker-config, ... }
data:
  broker.env: |
    CRABKA_LISTEN_ADDR=0.0.0.0:9092
  broker.properties: |
    log.retention.hours=24
    num.partitions=3
```

When `spec.config` is empty or `None`, `broker.properties` is omitted (so the diff is clean for "no config" clusters).

Serialization rules:
- Entries sorted by key (BTreeMap iteration is sorted — deterministic by construction).
- Each line is `key=value` followed by `\n`.
- No escaping; the operator passes the value through verbatim. Broker-side parsing handles its own escaping when it eventually consumes the file.

---

## 4. Hash propagation

```
Kafka.spec.config  --(serialize)-->  ConfigMap.data["broker.properties"]
                                     |
                                     v
                              sha256(content)  =  <hash>
                                     |
                                     v
        Kafka.reconcile: patch each pool's metadata.labels["crabka.io/config-hash"] = <hash>
                                     |
                                     v
        KafkaNodePool.reconcile: read pool.metadata.labels["crabka.io/config-hash"]
                                     |
                                     v
        StatefulSet.spec.template.metadata.annotations["crabka.io/config-hash"] = <hash>
                                     |
                                     v
                              K8s rolls pod(s)
```

Why route through the pool's labels rather than directly to the StatefulSet's pod-template annotation?

- The pool reconciler owns the StatefulSet (SSA field manager). Direct StatefulSet patches from the Kafka reconciler would conflict on field ownership.
- Pool label changes already trigger pool reconciles via `Controller::owns`. Reusing that pattern is free.
- One source of truth: the pool's label is the contract between the two reconcilers.

### Compute helper

```rust
// in controller/common.rs
pub(crate) fn config_hash(broker_properties: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(broker_properties.as_bytes());
    format!("{:x}", h.finalize())
}
```

(Uses the workspace's existing `sha2` dep — already pulled in by `crabka-security`.)

---

## 5. Reconciler changes

### `controller/kafka.rs`

1. Add a `serialize_broker_properties(spec: &KafkaSpec) -> String` helper.
2. Pass the rendered content to `render_configmap` (so the ConfigMap and the hash use the same string).
3. After applying the ConfigMap, compute `hash = config_hash(&content)`.
4. In the existing pool-adoption loop, the patch payload now sets BOTH `ownerReferences` AND `labels["crabka.io/config-hash"] = hash`. Use a single SSA patch per pool — operator's field manager owns both keys.
5. After listing pools, derive a `rolling` boolean by checking each pool's StatefulSet status (via a GET on `<parent>-<pool>` in the pool's namespace). Compose the second `KafkaStatus.conditions` entry accordingly.
   - To avoid a flurry of GETs, we read `pool.status.{replicas, ready_replicas}` and infer `rolling = pool_count > 0 && (ready_replicas < replicas)`. This is a proxy — the StatefulSet's `currentRevision != updateRevision` is the precise signal, but `ready_replicas < replicas` is the one we already have without extra I/O. Good enough for slice 21.

### `controller/kafka_node_pool.rs`

Inside `render_statefulset`, after the existing operator-owned `pod_annotations` map population:

```rust
if let Some(hash) = pool.metadata.labels.as_ref()
    .and_then(|l| l.get("crabka.io/config-hash"))
{
    pod_annotations.insert("crabka.io/config-hash".into(), hash.clone());
}
```

The annotation is included in `template_meta` as before. K8s rolls on annotation diff.

### `controller/common.rs`

- Add `pub(crate) fn config_hash(content: &str) -> String` (see § 4).

---

## 6. Owner-ref + label patch (single SSA call)

The slice-20 `adopt_pools` currently SSA-patches `metadata.ownerReferences` only. Slice 21 extends the same patch to set the label:

```rust
let patch_body = json!({
    "apiVersion": KafkaNodePool::api_version(&()),
    "kind": KafkaNodePool::kind(&()),
    "metadata": {
        "ownerReferences": [owner],
        "labels": { "crabka.io/config-hash": hash },
    }
});
```

SSA semantics ensure:
- Operator's field manager owns both keys.
- User-applied labels on the pool (e.g. `crabka.io/cluster=demo`) stay intact (different field manager).
- Idempotent: same hash → no observed change.

---

## 7. Testing

### Unit tests

- `crd::kafka::tests::spec_carries_config` — round-trip `Kafka` with `spec.config` populated.
- `controller::common::tests::config_hash_is_sha256_hex` — known string → known hash.
- `controller::common::tests::config_hash_empty_string` — sentinel sha256 of "".
- `controller::kafka::tests::serialize_broker_properties_sorted` — `BTreeMap` -> sorted `key=value` lines.
- `controller::kafka::tests::serialize_broker_properties_none_is_empty_string` — no config → "".
- `controller::kafka::tests::rolling_condition_when_pool_partial` — partial pool → `Rolling=True`.
- `controller::kafka::tests::rolling_condition_when_pool_stable` — full pool → `Rolling=False`.
- `controller::kafka_node_pool::tests::render_statefulset_propagates_config_hash_from_label` — pool label set → annotation set.
- `controller::kafka_node_pool::tests::render_statefulset_no_config_hash_when_label_absent` — no label → no annotation.

### Integration tests

`tests/reconcile_kafka.rs`:
- `kafka_writes_broker_properties_data_when_config_set` — Kafka with `spec.config`; assert the rendered ConfigMap PATCH body includes a `broker.properties` data entry with sorted lines.
- `kafka_patches_pool_label_with_config_hash` — assert the pool adopt PATCH body contains `metadata.labels["crabka.io/config-hash"]` with the expected sha256.
- `kafka_status_includes_rolling_condition_partial` — pool list shows a partial pool; assert the status PATCH body's conditions include `Rolling=True, reason=RollingUpdate`.
- `kafka_status_includes_rolling_condition_stable` — full pool; `Rolling=False, reason=Stable`.

The existing slice-20 tests stay green (config_hash absent → no broker.properties key, status PATCH gets the Ready condition only as before).

### E2E (kind)

Two new probes layered onto the existing slice-20c workflow:

1. Modify the apply step to set initial `spec.config: { log.retention.hours: "24" }`.
2. After the slice-20c smoke step (broker binary + pod template), add a "Rolling restart on config change" step:
   ```yaml
   - name: Smoke — config change rolls broker pod
     run: |
       set -e
       initial_uid=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.metadata.uid}')
       initial_rev=$(kubectl get sts demo-brokers -n default -o jsonpath='{.status.currentRevision}')

       kubectl patch kafka demo -n default --type=merge -p '{"spec":{"config":{"log.retention.hours":"48"}}}'

       # Wait up to 90s for the pod's UID to change (replacement).
       for i in $(seq 1 45); do
         current_uid=$(kubectl get pod demo-brokers-0 -n default -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
         echo "attempt $i: uid=$current_uid (initial=$initial_uid)"
         if [ -n "$current_uid" ] && [ "$current_uid" != "$initial_uid" ]; then
           break
         fi
         sleep 2
       done

       # Wait for the new pod to become Ready.
       kubectl wait --for=condition=Ready -n default pod/demo-brokers-0 --timeout=90s

       final_rev=$(kubectl get sts demo-brokers -n default -o jsonpath='{.status.currentRevision}')
       [ "$final_rev" != "$initial_rev" ] || { echo "::error::sts revision didn't change"; exit 1; }
       echo "rolled: $initial_rev -> $final_rev"
   ```

The existing GC step and diagnostics block stay unchanged (the pool name + StatefulSet name didn't change).

---

## 8. File structure

```
crates/operator/src/
├── crd/kafka.rs                  # MODIFIED — spec.config
├── controller/common.rs          # MODIFIED — config_hash helper
├── controller/kafka.rs           # MODIFIED — serialize, hash, label patch, Rolling cond
├── controller/kafka_node_pool.rs # MODIFIED — propagate annotation from label
crates/operator/tests/
├── reconcile_kafka.rs            # MODIFIED — config + hash + rolling tests
deploy/crds/
├── crabka.io_kafkas.yaml         # REGENERATED
.github/workflows/
├── operator-e2e.yml              # MODIFIED — patch+verify roll step
```

Implementation plan: **~5 tasks across 3 batches**.

- **Batch 1 (parallel):** T1 CRD `spec.config`, T2 `config_hash` helper in common.rs.
- **Batch 2 (sequential):** T3 Kafka reconciler (serialize, label patch, Rolling cond) — depends on T1+T2; T4 pool reconciler (propagate annotation) — depends on T1.
- **Batch 3 (sequential):** T5 regen CRD + e2e workflow + final verification.

---

## 9. Acceptance criteria

1. `cargo test -p crabka-operator` green (existing + new tests).
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` passes.
4. CRD regen stable.
5. operator-e2e: applying `spec.config` change observes pod UID change AND StatefulSet revision change within 90 s; new pod reaches Ready.

---

## 10. Open questions resolved

- **Why route the hash through pool labels rather than StatefulSet annotations directly?** Field-manager ownership: the pool reconciler owns the StatefulSet (via SSA). The Kafka reconciler can't patch the same object cleanly. Pool labels are operator-owned but cheap to patch from the Kafka side; the pool reconciler picks them up via its existing watch.
- **Why sha256 specifically?** Workspace already depends on `sha2`. Faster hashes (xxhash) would need a new dep for marginal benefit.
- **Why not trigger on every ConfigMap edit (i.e., watch `ConfigMap` for changes too)?** The operator owns the ConfigMap via SSA — user edits to operator-owned fields are reverted on next reconcile. The trigger should be `Kafka.spec.config`, which is the user-facing surface.
- **What happens on a multi-replica pool (slice 20a)?** The same annotation propagation works; K8s `StatefulSet` rolls one pod at a time with `partition: 0` and default `maxUnavailable`. ISR-aware ordering is a separate slice (21b).
