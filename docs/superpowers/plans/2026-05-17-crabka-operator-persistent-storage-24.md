# Crabka Operator Slice 24 — Persistent storage on `KafkaNodePool` Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KafkaNodePool.spec.storage` as a tagged-union over `Ephemeral` (default; current `emptyDir` behavior) and `PersistentClaim {size, class, deleteClaim}` (single PVC per pod via `volumeClaimTemplates`). Monotonic resize allowed; type/class changes rejected.

**Architecture:** Renderer in `controller/kafka_node_pool.rs` switches between `volumes: [{emptyDir}]` and a `volumeClaimTemplates` entry on the `StatefulSet`. The PVC retention policy honors `deleteClaim`. Validation runs in two phases: static (size parseable as a positive Quantity) before any I/O; monotonic (against the live StatefulSet's existing `volumeClaimTemplates`) right before SSA-apply. The reconcile loop gains one extra GET per tick on the StatefulSet to support the monotonic check. PVC labels inherit pod-template labels so the existing slice-20 GC selector reaches them.

**Tech Stack:** Rust 1.95.0, `kube-rs` 3.x, `k8s-openapi` (apps/v1 StatefulSet, core/v1 PersistentVolumeClaim), `serde`, `serde_json`, `schemars`. No new workspace dependencies — `parse_quantity` is in-tree (~50 lines).

**Spec:** [`docs/superpowers/specs/2026-05-17-crabka-operator-persistent-storage-24-design.md`](../specs/2026-05-17-crabka-operator-persistent-storage-24-design.md)

**Conventions:**
- `[lints] workspace = true`; clippy `pedantic` warn-by-default.
- `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are CI gates.
- Per CLAUDE.md: greenfield — no backwards-compat shims.

---

## Files

Created or modified by this slice (in dependency order):

| Path | Action | Responsibility |
|---|---|---|
| `crates/operator/src/crd/kafka_node_pool.rs` | Modify | Add `Storage` enum + `PersistentClaimSpec` types; append `storage: Option<Storage>` to `KafkaNodePoolSpec` |
| `crates/operator/src/controller/common.rs` | Modify | Add `parse_quantity` helper + unit tests |
| `crates/operator/src/controller/kafka_node_pool.rs` | Modify | Switch renderer between emptyDir / volumeClaimTemplates; add `validate_storage_change`; wire validation into `reconcile`; add unit tests |
| `crates/operator/tests/reconcile_pool.rs` | Modify | 2 new integration tests; extend shared mock harness to model `volumeClaimTemplates` in the StatefulSet GET response |
| `deploy/crds/crabka.io_kafkanodepools.yaml` | Regenerate | `crabka-operator gen-crds deploy/crds` |
| `.github/workflows/operator-e2e.yml` | Modify | Apply with `storage.type=PersistentClaim`; smoke probe asserting PVC `Bound`; extend GC step's success condition to include PVCs |

---

## Batch overview

Per CLAUDE.md: dispatch batches in parallel where the per-task file sets don't overlap.

| Batch | Tasks | File overlap | Parallel? |
|---|---|---|---|
| 1 | T1, T2 | `crd/kafka_node_pool.rs` vs `controller/common.rs` — disjoint | yes |
| 2 | T3 then T4 | Both modify `controller/kafka_node_pool.rs` | sequential |
| 3 | T5, T6 | `deploy/crds/` + `.github/workflows/` — disjoint | yes |
| 4 | T7 | Verify only | — |

---

## Task 1 — `Storage` enum + `PersistentClaimSpec` on `KafkaNodePoolSpec`

**Files:**
- Modify: `crates/operator/src/crd/kafka_node_pool.rs`

> Goal: introduce the new types and append the `storage` field. Existing tests should still pass with `storage: None` filled in. New round-trip tests cover both enum variants.

- [ ] **Step 1: Define the enum and config struct**

Open `crates/operator/src/crd/kafka_node_pool.rs`. Below the existing `NodeRole` enum, add:

```rust
/// Storage configuration for the pool's pods. Slice 24 supports two
/// variants:
/// - `Ephemeral` (or field absent) — `emptyDir` volume, no PVC. Matches
///   slice 19/20 behavior; suitable for dev clusters.
/// - `PersistentClaim` — single PVC per pod via the StatefulSet's
///   `volumeClaimTemplates`. Production-shaped.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type")]
pub enum Storage {
    Ephemeral,
    PersistentClaim(PersistentClaimSpec),
}

/// `PersistentClaim` configuration. Mirrors Strimzi's
/// `KafkaNodePool.spec.storage` flat shape.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentClaimSpec {
    /// K8s `Quantity` (e.g., `"10Gi"`, `"500Mi"`). Validated at
    /// reconcile time.
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`.
    /// Default `false` (Retain) is the safe option.
    #[serde(default)]
    pub delete_claim: bool,
}
```

- [ ] **Step 2: Add the `storage` field to `KafkaNodePoolSpec`**

Append after the existing `template` field:

```rust
    /// Storage configuration. `None` (field absent) → emptyDir (the
    /// slice 19/20 default). See [`Storage`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<Storage>,
```

- [ ] **Step 3: Re-export the new public types**

In `crates/operator/src/crd/mod.rs`, add `Storage` and `PersistentClaimSpec` to the existing `pub use kafka_node_pool::{...};` re-export list.

- [ ] **Step 4: Update existing `KafkaNodePoolSpec` literals to pass `storage: None`**

The new field will break every test fixture and integration-test JSON body that constructs `KafkaNodePoolSpec { ... }` literally. Search and patch:

```bash
grep -rn "KafkaNodePoolSpec {" crates/operator/src crates/operator/tests
```

Expect hits in:
- `crates/operator/src/controller/kafka.rs` (test fixture `pool_with_status`)
- `crates/operator/src/controller/kafka_node_pool.rs` (test fixture `pool_fixture`)

For each, add `storage: None,` to the literal.

- [ ] **Step 5: Add round-trip + default test**

Inside `#[cfg(test)] mod tests` in `crd/kafka_node_pool.rs`:

```rust
#[test]
fn storage_ephemeral_round_trips_through_json() {
    let pool = KafkaNodePool::new(
        "brokers",
        KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas: 1,
            node_id_start: 0,
            image: None,
            resources: None,
            template: None,
            storage: Some(Storage::Ephemeral),
        },
    );
    let json = serde_json::to_string(&pool).unwrap();
    assert!(
        json.contains("\"storage\":{\"type\":\"Ephemeral\"}"),
        "expected flat tagged shape, got: {json}"
    );
    let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
    assert_eq!(back.spec, pool.spec);
}

#[test]
fn storage_persistent_claim_round_trips_through_json() {
    let pool = KafkaNodePool::new(
        "brokers",
        KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas: 1,
            node_id_start: 0,
            image: None,
            resources: None,
            template: None,
            storage: Some(Storage::PersistentClaim(PersistentClaimSpec {
                size: "10Gi".into(),
                class: Some("fast-ssd".into()),
                delete_claim: true,
            })),
        },
    );
    let json = serde_json::to_string(&pool).unwrap();
    assert!(json.contains("\"type\":\"PersistentClaim\""), "got: {json}");
    assert!(json.contains("\"size\":\"10Gi\""), "got: {json}");
    assert!(json.contains("\"class\":\"fast-ssd\""), "got: {json}");
    assert!(json.contains("\"deleteClaim\":true"), "got: {json}");
    let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
    assert_eq!(back.spec, pool.spec);
}

#[test]
fn spec_defaults_storage_to_none() {
    let json = r#"{"roles":["Controller","Broker"],"nodeIdStart":0}"#;
    let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
    assert!(spec.storage.is_none());
}
```

- [ ] **Step 6: Verify**

```bash
cargo test -p crabka-operator --lib crd::kafka_node_pool
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: 3 new tests pass alongside the existing 4 (`crd_metadata_is_correct`, `round_trips_through_json`, `spec_defaults_replicas_to_one`, `pod_template_round_trips_through_json`); clippy clean.

If the wider crate fails to compile (because Task 3 hasn't landed yet and `validate_storage_change` is called somewhere), that's OK — the `--lib crd::kafka_node_pool` test target is what matters here. Use `cargo check -p crabka-operator --lib 2>&1 | grep -E '^error' | head -5` and confirm errors are only in `controller/kafka_node_pool.rs` (Task 3/4 territory) or `controller/common.rs` (Task 2 territory).

- [ ] **Step 7: Commit**

```bash
git add crates/operator/src/crd/ crates/operator/src/controller/
git commit -m "feat(operator): KafkaNodePool.spec.storage CRD types (slice 24, task 1)"
```

---

## Task 2 — `parse_quantity` helper in `controller/common.rs`

**Files:**
- Modify: `crates/operator/src/controller/common.rs`

> Parallel with Task 1 — disjoint file. Adds a small K8s `Quantity` parser used by the slice-24 size validation.

- [ ] **Step 1: Write the failing tests first (TDD)**

Add at the bottom of `crates/operator/src/controller/common.rs`:

```rust
#[cfg(test)]
mod parse_quantity_tests {
    use super::parse_quantity;

    #[test]
    fn quantity_parse_binary_suffixes() {
        assert_eq!(parse_quantity("1Ki").unwrap(), 1024);
        assert_eq!(parse_quantity("512Mi").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_quantity("10Gi").unwrap(), 10 * 1024 * 1024 * 1024);
    }

    #[test]
    fn quantity_parse_decimal_suffixes() {
        assert_eq!(parse_quantity("1K").unwrap(), 1_000);
        assert_eq!(parse_quantity("500M").unwrap(), 500_000_000);
        assert_eq!(parse_quantity("10G").unwrap(), 10_000_000_000);
    }

    #[test]
    fn quantity_parse_decimal_mantissa() {
        // 1.5Gi = 1.5 * 1024^3 = 1,610,612,736
        assert_eq!(parse_quantity("1.5Gi").unwrap(), 1_610_612_736);
    }

    #[test]
    fn quantity_parse_no_suffix_is_bytes() {
        assert_eq!(parse_quantity("1024").unwrap(), 1024);
    }

    #[test]
    fn quantity_parse_rejects_garbage() {
        assert!(parse_quantity("").is_err());
        assert!(parse_quantity("banana").is_err());
        assert!(parse_quantity("1.5x").is_err());
        assert!(parse_quantity("Gi").is_err());
        // No scientific notation:
        assert!(parse_quantity("1e3").is_err());
    }

    #[test]
    fn quantity_parse_zero_and_negative_are_errors() {
        assert!(parse_quantity("0").is_err());
        assert!(parse_quantity("0Gi").is_err());
        assert!(parse_quantity("-10Gi").is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-operator --lib controller::common::parse_quantity_tests 2>&1 | tail -5
```

Expected: build errors (`parse_quantity` not defined). That's the failure shape we want.

- [ ] **Step 3: Implement the parser**

Add to `crates/operator/src/controller/common.rs` (place above the `#[cfg(test)]` block):

```rust
/// Parse a K8s `Quantity` string into a comparable byte count.
///
/// Accepts:
/// - Binary suffixes: `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei` (1 Ki = 1024).
/// - Decimal suffixes: `K`, `M`, `G`, `T`, `P`, `E` (1 K = 1000).
/// - Bare numbers (no suffix → bytes).
/// - Integer or decimal mantissa (`1.5Gi`).
///
/// Rejects: scientific notation, negative numbers, zero, empty
/// strings, or any value that doesn't match `<mantissa><suffix?>`.
///
/// Returns the byte count as `i128` (1.5Pi fits with headroom for
/// arithmetic). Slice 24 only uses the result for ordered comparison
/// — we never round-trip back to a string.
///
/// # Errors
///
/// Returns a static `&str` describing the parse failure.
pub(crate) fn parse_quantity(s: &str) -> Result<i128, &'static str> {
    if s.is_empty() {
        return Err("empty quantity string");
    }

    // Split mantissa from suffix. K8s suffixes are 1-2 ASCII chars.
    let (mantissa_str, multiplier): (&str, i128) = if let Some(rest) = s.strip_suffix("Ki") {
        (rest, 1_024)
    } else if let Some(rest) = s.strip_suffix("Mi") {
        (rest, 1_024_i128.pow(2))
    } else if let Some(rest) = s.strip_suffix("Gi") {
        (rest, 1_024_i128.pow(3))
    } else if let Some(rest) = s.strip_suffix("Ti") {
        (rest, 1_024_i128.pow(4))
    } else if let Some(rest) = s.strip_suffix("Pi") {
        (rest, 1_024_i128.pow(5))
    } else if let Some(rest) = s.strip_suffix("Ei") {
        (rest, 1_024_i128.pow(6))
    } else if let Some(rest) = s.strip_suffix('K') {
        (rest, 1_000)
    } else if let Some(rest) = s.strip_suffix('M') {
        (rest, 1_000_000)
    } else if let Some(rest) = s.strip_suffix('G') {
        (rest, 1_000_000_000)
    } else if let Some(rest) = s.strip_suffix('T') {
        (rest, 1_000_000_000_000)
    } else if let Some(rest) = s.strip_suffix('P') {
        (rest, 1_000_000_000_000_000)
    } else if let Some(rest) = s.strip_suffix('E') {
        (rest, 1_000_000_000_000_000_000)
    } else {
        // No suffix; the whole string is the mantissa.
        (s, 1)
    };

    if mantissa_str.is_empty() {
        return Err("missing numeric mantissa before suffix");
    }
    // Reject scientific notation explicitly (`1e3` would otherwise parse
    // as a float and slip through). The mantissa must not contain `e`
    // or `E` (decimal suffix `E` already consumed).
    if mantissa_str.contains(['e', 'E']) {
        return Err("scientific notation not supported");
    }
    // Reject negative.
    if mantissa_str.starts_with('-') {
        return Err("negative quantity rejected");
    }

    // Parse as f64 to handle decimal mantissas (`1.5Gi`). We multiply
    // by the integer multiplier and convert to i128, rounding down.
    // For our use case (storage comparison) this gives 53 bits of
    // mantissa precision — overkill for byte counts up to many EiB.
    let mantissa: f64 = mantissa_str
        .parse()
        .map_err(|_| "mantissa is not a valid number")?;
    if !mantissa.is_finite() {
        return Err("mantissa is not finite");
    }
    if mantissa <= 0.0 {
        return Err("quantity must be strictly positive");
    }

    // (mantissa * multiplier) -> i128 via f64. Loses precision for
    // very large values but the comparison invariant we need (mono-
    // tonicity for size changes) is unaffected by sub-byte rounding.
    let bytes = mantissa * multiplier as f64;
    if bytes > i128::MAX as f64 {
        return Err("quantity overflows i128");
    }
    Ok(bytes as i128)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p crabka-operator --lib controller::common::parse_quantity_tests
```

Expected: 6 tests pass.

- [ ] **Step 5: Lint + commit**

```bash
cargo clippy -p crabka-operator --lib -- -D warnings
git add crates/operator/src/controller/common.rs
git commit -m "feat(operator): parse_quantity helper for K8s Quantity strings (slice 24, task 2)"
```

---

## Task 3 — Renderer: switch volume between emptyDir / volumeClaimTemplates

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

> Sequencing: depends on Task 1 (the `Storage` type). Must come before Task 4 (which validates against the renderer's output).

The existing `render_statefulset` has hard-coded `volumes: [{name: data, emptyDir: {}}]` and no `volumeClaimTemplates`. Slice 24 conditionally switches between the two based on `pool.spec.storage`.

- [ ] **Step 1: Helper — build volume / volumeClaimTemplates from `Storage`**

Add this near `render_statefulset` (in the same `kafka_node_pool.rs` module, in the same scope as `render_init_container`):

```rust
/// Build the StatefulSet's pod-volume entry and (optionally) its
/// `volumeClaimTemplates` based on the pool's `Storage` setting. Returns
/// `(pod_volumes_json, volume_claim_templates_json_or_none)`.
///
/// - `None` or `Some(Ephemeral)` → `(emptyDir, None)`.
/// - `Some(PersistentClaim)` → `(no `data` entry in volumes, Some(template))`.
fn render_storage(
    storage: Option<&Storage>,
    pod_labels: &BTreeMap<String, String>,
) -> (serde_json::Value, Option<serde_json::Value>) {
    match storage {
        None | Some(Storage::Ephemeral) => {
            let volumes = json!([{ "name": "data", "emptyDir": {} }]);
            (volumes, None)
        }
        Some(Storage::PersistentClaim(pc)) => {
            let mut template = json!({
                "metadata": {
                    "name": "data",
                    "labels": pod_labels,
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": { "storage": pc.size }
                    }
                }
            });
            if let Some(class) = pc.class.as_ref() {
                template["spec"]["storageClassName"] = serde_json::Value::String(class.clone());
            }
            // Empty `volumes` for `data` — the StatefulSet controller
            // mounts the PVC under the same name automatically.
            (json!([]), Some(template))
        }
    }
}

/// Build the StatefulSet's `persistentVolumeClaimRetentionPolicy`
/// block when storage is `PersistentClaim`. Returns `None` for
/// `Ephemeral` (no PVCs to retain).
fn render_pvc_retention_policy(storage: Option<&Storage>) -> Option<serde_json::Value> {
    match storage {
        Some(Storage::PersistentClaim(pc)) => Some(json!({
            "whenDeleted": if pc.delete_claim { "Delete" } else { "Retain" },
            "whenScaled": "Retain",
        })),
        _ => None,
    }
}
```

Add `Storage` to the `use crate::crd::...` import at the top of the file.

- [ ] **Step 2: Wire `render_storage` into `render_statefulset`**

Locate the existing inline `"volumes": [{ "name": "data", "emptyDir": {} }]` in `render_statefulset`'s `json!({"spec": {...}})` block. Replace the block so it consults `render_storage` and conditionally includes `volumeClaimTemplates` and `persistentVolumeClaimRetentionPolicy`.

Concretely, the existing rendered block ends with the StatefulSet body assembled via `serde_json::from_value(json!({...}))?`. Restructure it like this (showing only the changed parts; preserve all existing fields):

```rust
let (pod_volumes, volume_claim_templates) = render_storage(pool.spec.storage.as_ref(), &pod_labels);
let retention_policy = render_pvc_retention_policy(pool.spec.storage.as_ref());

let mut sts_spec = json!({
    "serviceName": service_name,
    "replicas": pool.spec.replicas,
    "podManagementPolicy": "Parallel",
    "selector": { "matchLabels": selector },
    "template": {
        "metadata": template_meta,
        "spec": pod_spec_with_data_volume(pod_spec, pod_volumes),
    }
});
if let Some(vct) = volume_claim_templates {
    sts_spec["volumeClaimTemplates"] = json!([vct]);
}
if let Some(policy) = retention_policy {
    sts_spec["persistentVolumeClaimRetentionPolicy"] = policy;
}

let sts: StatefulSet = serde_json::from_value(json!({
    "metadata": {
        "name": sts_name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [owner_ref::<KafkaNodePool>(pool)?],
    },
    "spec": sts_spec,
}))?;
Ok(sts)
```

And add the small helper that merges `pod_volumes` into the existing `pod_spec` block (which already has `volumes: [{name: data, emptyDir: {}}]` baked in — we need to replace, not append):

```rust
/// Merge the rendered `pod_volumes` into `pod_spec`'s `volumes` array,
/// replacing any pre-existing entry. Slice-24 storage variants set the
/// `data` volume; non-`data` volumes (none today) would be preserved
/// if they existed.
fn pod_spec_with_data_volume(
    mut pod_spec: serde_json::Value,
    pod_volumes: serde_json::Value,
) -> serde_json::Value {
    pod_spec["volumes"] = pod_volumes;
    pod_spec
}
```

(The existing inline `"volumes": [{ "name": "data", "emptyDir": {} }]` inside `pod_spec` becomes redundant — leave it in place so `pod_spec_with_data_volume` simply overwrites it. This keeps the diff small.)

- [ ] **Step 3: Renderer unit tests — emptyDir paths (both `None` and `Some(Ephemeral)`)**

Add to `#[cfg(test)] mod tests` in `kafka_node_pool.rs`:

```rust
#[test]
fn render_statefulset_emptydir_when_storage_none() {
    let pool = pool_fixture("brokers", "demo", 1);
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let spec = sts.spec.unwrap();
    // No volumeClaimTemplates.
    assert!(spec.volume_claim_templates.is_none() || spec.volume_claim_templates.as_ref().unwrap().is_empty());
    // `data` volume in pod template is emptyDir.
    let volumes = spec.template.spec.unwrap().volumes.unwrap();
    let data_vol = volumes.iter().find(|v| v.name == "data").expect("data volume present");
    assert!(data_vol.empty_dir.is_some(), "data volume must be emptyDir; got {data_vol:?}");
}

#[test]
fn render_statefulset_emptydir_when_storage_ephemeral() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::Ephemeral);
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let spec = sts.spec.unwrap();
    assert!(spec.volume_claim_templates.is_none() || spec.volume_claim_templates.as_ref().unwrap().is_empty());
    let volumes = spec.template.spec.unwrap().volumes.unwrap();
    let data_vol = volumes.iter().find(|v| v.name == "data").unwrap();
    assert!(data_vol.empty_dir.is_some());
}
```

You'll need imports at the top of the `tests` module:

```rust
use crate::crd::{PersistentClaimSpec, Storage};
```

- [ ] **Step 4: Renderer unit tests — PersistentClaim path**

```rust
#[test]
fn render_statefulset_volume_claim_template_when_persistent() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "10Gi".into(),
        class: Some("fast-ssd".into()),
        delete_claim: false,
    }));
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let spec = sts.spec.unwrap();
    // No emptyDir entry for `data` in the pod-template volumes.
    let volumes = spec.template.spec.as_ref().unwrap().volumes.as_ref();
    if let Some(vols) = volumes {
        assert!(
            vols.iter().all(|v| v.name != "data" || v.empty_dir.is_none()),
            "expected no emptyDir for data; got {vols:?}"
        );
    }
    // volumeClaimTemplates has the `data` entry.
    let vct = spec.volume_claim_templates.unwrap();
    assert_eq!(vct.len(), 1);
    let data_pvc = &vct[0];
    assert_eq!(data_pvc.metadata.name.as_deref(), Some("data"));
    let pvc_spec = data_pvc.spec.as_ref().unwrap();
    assert_eq!(pvc_spec.access_modes.as_deref(), Some(["ReadWriteOnce".to_string()].as_slice()));
    let req = pvc_spec.resources.as_ref().unwrap().requests.as_ref().unwrap();
    assert_eq!(req.get("storage").map(|q| q.0.as_str()), Some("10Gi"));
    assert_eq!(pvc_spec.storage_class_name.as_deref(), Some("fast-ssd"));
}

#[test]
fn render_statefulset_no_storage_class_when_class_absent() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "1Gi".into(),
        class: None,
        delete_claim: false,
    }));
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let pvc_spec = sts.spec.unwrap().volume_claim_templates.unwrap()[0].spec.clone().unwrap();
    assert!(pvc_spec.storage_class_name.is_none(), "must omit storageClassName when class is None");
}

#[test]
fn render_statefulset_pvc_labels_inherit_pod_labels() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "1Gi".into(),
        class: None,
        delete_claim: false,
    }));
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let labels = sts.spec.unwrap().volume_claim_templates.unwrap()[0]
        .metadata
        .labels
        .clone()
        .expect("PVC has labels");
    assert_eq!(labels.get("app.kubernetes.io/instance").map(String::as_str), Some("demo"));
    assert_eq!(labels.get("crabka.io/pool").map(String::as_str), Some("brokers"));
}
```

- [ ] **Step 5: Renderer unit tests — retention policy**

```rust
#[test]
fn render_statefulset_retention_policy_delete_when_delete_claim_true() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "1Gi".into(),
        class: None,
        delete_claim: true,
    }));
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let policy = sts.spec.unwrap().persistent_volume_claim_retention_policy.unwrap();
    assert_eq!(policy.when_deleted.as_deref(), Some("Delete"));
    assert_eq!(policy.when_scaled.as_deref(), Some("Retain"));
}

#[test]
fn render_statefulset_retention_policy_retain_when_delete_claim_false() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "1Gi".into(),
        class: None,
        delete_claim: false,
    }));
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    let policy = sts.spec.unwrap().persistent_volume_claim_retention_policy.unwrap();
    assert_eq!(policy.when_deleted.as_deref(), Some("Retain"));
    assert_eq!(policy.when_scaled.as_deref(), Some("Retain"));
}

#[test]
fn render_statefulset_no_retention_policy_when_ephemeral() {
    let pool = pool_fixture("brokers", "demo", 1);
    // storage = None
    let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
    assert!(sts.spec.unwrap().persistent_volume_claim_retention_policy.is_none());
}
```

- [ ] **Step 6: Verify**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator --lib controller::kafka_node_pool::tests
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: ~16 existing pool tests still pass + 8 new = 24 tests; clippy clean.

If `Volume.empty_dir`, `VolumeClaimTemplate`, or `PersistentVolumeClaimRetentionPolicy` field names differ from what's shown (k8s-openapi 0.27 type details), adjust the test assertions to match the actual struct field names. Use `cargo doc -p k8s-openapi` or grep `k8s_openapi::api::apps::v1::StatefulSetSpec` definitions if needed.

- [ ] **Step 7: Commit**

```bash
git add crates/operator/src/controller/kafka_node_pool.rs
git commit -m "feat(operator): render PVC volumeClaimTemplates + retention policy (slice 24, task 3)"
```

---

## Task 4 — `validate_storage_change` + wire into `reconcile`

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

> Sequencing: depends on Task 3 (the renderer change) — the reconcile flow needs to GET the live StatefulSet before SSA-apply, and the validation reads `volumeClaimTemplates` from it.

- [ ] **Step 1: Extend `PoolValidationError`**

In `kafka_node_pool.rs`, find the existing `PoolValidationError` enum (added in slice 20). Append:

```rust
    /// `pc.size` doesn't parse as a K8s Quantity, or is non-positive.
    #[error("spec.storage.size={0:?} is not a valid positive Quantity ({1})")]
    StorageSizeInvalid(String, &'static str),
    /// Switch between `Ephemeral` and `PersistentClaim` on an existing pool.
    #[error("spec.storage.type changed from {from} to {to}: immutable")]
    StorageTypeChanged { from: &'static str, to: &'static str },
    /// `pc.class` changed on an existing pool.
    #[error("spec.storage.class changed from {from:?} to {to:?}: immutable")]
    StorageClassChanged {
        from: Option<String>,
        to: Option<String>,
    },
    /// Size decreased.
    #[error("spec.storage.size decrease from {current} to {desired}: shrink not allowed")]
    StorageShrinkNotAllowed {
        current: String,
        desired: String,
    },
```

- [ ] **Step 2: Map new variants to condition reasons**

Find `condition_for_validation_error` (slice-20 helper). Add the new arms:

```rust
        PoolValidationError::StorageSizeInvalid(value, why) => (
            "StorageSizeInvalid",
            format!("spec.storage.size={value:?} ({why})"),
        ),
        PoolValidationError::StorageTypeChanged { from, to } => (
            "StorageImmutable",
            format!("spec.storage.type changed from {from} to {to}"),
        ),
        PoolValidationError::StorageClassChanged { from, to } => (
            "StorageImmutable",
            format!("spec.storage.class changed from {from:?} to {to:?}"),
        ),
        PoolValidationError::StorageShrinkNotAllowed { current, desired } => (
            "StorageImmutable",
            format!("spec.storage.size {current} -> {desired} (shrink rejected)"),
        ),
```

- [ ] **Step 3: Static validation in `validate`**

Find the existing `validate(pool: &KafkaNodePool) -> Result<(), PoolValidationError>` function and append the storage check at the end (after the `node_id_start` range check):

```rust
    // Slice 24: validate spec.storage.size parses and is positive.
    if let Some(Storage::PersistentClaim(pc)) = pool.spec.storage.as_ref() {
        common::parse_quantity(&pc.size).map_err(|why| {
            PoolValidationError::StorageSizeInvalid(pc.size.clone(), why)
        })?;
    }
```

Make sure `Storage` is in scope (add `use crate::crd::Storage;` to imports).

- [ ] **Step 4: Implement `validate_storage_change`**

Add a new module-private function:

```rust
/// Monotonic validation against the live StatefulSet's observed
/// `volumeClaimTemplates`. Returns `Ok(())` when no live StatefulSet
/// exists (first reconcile — any spec is acceptable) or when the
/// desired and observed agree on the immutable fields.
///
/// Rejections (all map to `Ready=False, reason=StorageImmutable`):
/// - `Ephemeral ↔ PersistentClaim` switch.
/// - `class` change.
/// - `size` decrease.
fn validate_storage_change(
    desired: Option<&Storage>,
    observed_template: Option<&PersistentVolumeClaim>,
) -> Result<(), PoolValidationError> {
    let observed = observed_template.map(observed_storage_from_pvc_template);
    let desired_kind = storage_kind(desired);
    let observed_kind = observed.as_ref().map(storage_kind);

    // First reconcile (no observed state) — any spec is OK.
    let Some(observed_kind) = observed_kind else { return Ok(()) };

    if desired_kind != observed_kind {
        return Err(PoolValidationError::StorageTypeChanged {
            from: observed_kind,
            to: desired_kind,
        });
    }

    // If both are Ephemeral, no further checks.
    let (
        Some(Storage::PersistentClaim(desired_pc)),
        Some(Storage::PersistentClaim(observed_pc)),
    ) = (desired, observed.as_ref()) else {
        return Ok(());
    };

    if desired_pc.class != observed_pc.class {
        return Err(PoolValidationError::StorageClassChanged {
            from: observed_pc.class.clone(),
            to: desired_pc.class.clone(),
        });
    }

    // Size: increase OK, decrease rejected.
    let observed_bytes = common::parse_quantity(&observed_pc.size).unwrap_or(0);
    let desired_bytes = common::parse_quantity(&desired_pc.size).unwrap_or(0);
    if desired_bytes < observed_bytes {
        return Err(PoolValidationError::StorageShrinkNotAllowed {
            current: observed_pc.size.clone(),
            desired: desired_pc.size.clone(),
        });
    }

    Ok(())
}

/// Reconstruct a `Storage` value from the live StatefulSet's `data`
/// volumeClaimTemplate (if present). Returns `Storage::Ephemeral`
/// when the live StatefulSet has no `data` PVC template.
fn observed_storage_from_pvc_template(pvc: &PersistentVolumeClaim) -> Storage {
    let Some(spec) = pvc.spec.as_ref() else { return Storage::Ephemeral };
    let size = spec
        .resources
        .as_ref()
        .and_then(|r| r.requests.as_ref())
        .and_then(|m| m.get("storage"))
        .map(|q| q.0.clone())
        .unwrap_or_default();
    let class = spec.storage_class_name.clone();
    // delete_claim is not reflected in the PVC template; the
    // monotonic validator doesn't check it (deleteClaim is mutable
    // by design — spec section 4).
    Storage::PersistentClaim(PersistentClaimSpec {
        size,
        class,
        delete_claim: false,
    })
}

fn storage_kind(s: Option<&Storage>) -> &'static str {
    match s {
        None | Some(Storage::Ephemeral) => "Ephemeral",
        Some(Storage::PersistentClaim(_)) => "PersistentClaim",
    }
}
```

Add `use k8s_openapi::api::core::v1::PersistentVolumeClaim;` to imports. Add `use crate::crd::PersistentClaimSpec;` if not already present.

- [ ] **Step 5: Wire monotonic validation into `reconcile`**

Find the existing `reconcile` flow. Today the StatefulSet GET happens after the SSA-apply (for status reading only). Slice 24 needs a pre-apply GET as well. Locate the SSA-apply block:

```rust
    let sts = render_statefulset(&parent, &pool, &broker_image)?;
    apply_object(&ctx.client, &ns, &sts).await?;
    // ... existing post-apply GET for status ...
```

Restructure to:

```rust
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let sts_name = format!("{}-{}", kafka_name, pool_name);

    // Pre-apply GET: capture the live StatefulSet (or None on first
    // reconcile) for monotonic-storage validation.
    let observed_sts = sts_api.get_opt(&sts_name).await?;
    let observed_pvc_template = observed_sts
        .as_ref()
        .and_then(|s| s.spec.as_ref())
        .and_then(|spec| spec.volume_claim_templates.as_ref())
        .and_then(|templates| templates.iter().find(|t| t.metadata.name.as_deref() == Some("data")));

    // Slice 24 monotonic-storage validation.
    if let Err(err) = validate_storage_change(pool.spec.storage.as_ref(), observed_pvc_template) {
        let cond = condition_for_validation_error(&err);
        patch_status_for_pool(&pool_api, &name, cond).await?;
        return Ok(Action::await_change());
    }

    // ... existing render + SSA-apply block ...
    let sts = render_statefulset(&parent, &pool, &broker_image)?;
    apply_object(&ctx.client, &ns, &sts).await?;

    // Post-apply: read live state for the status mirror (existing
    // slice-20 path — kept distinct from the pre-apply GET so the
    // status reflects the rolled-out replicas).
    let live = sts_api.get_opt(&sts_name).await?;
    // ... existing status-derivation + patch ...
```

(The specific signatures of `apply_object` may want an `Api<StatefulSet>` rather than `&ctx.client + ns`; preserve whatever shape the slice-21 / slice-20 code already uses. The structural changes here are: add the pre-apply GET, call `validate_storage_change`.)

- [ ] **Step 6: Validation unit tests**

Add to `#[cfg(test)] mod tests` in `kafka_node_pool.rs`:

```rust
fn pvc_template(size: &str, class: Option<&str>) -> PersistentVolumeClaim {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "storage".to_string(),
        k8s_openapi::apimachinery::pkg::api::resource::Quantity(size.into()),
    );
    PersistentVolumeClaim {
        metadata: kube::core::ObjectMeta {
            name: Some("data".into()),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::core::v1::PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".into()]),
            resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                requests: Some(map),
                ..Default::default()
            }),
            storage_class_name: class.map(String::from),
            ..Default::default()
        }),
        status: None,
    }
}

fn pc(size: &str, class: Option<&str>) -> Storage {
    Storage::PersistentClaim(PersistentClaimSpec {
        size: size.into(),
        class: class.map(String::from),
        delete_claim: false,
    })
}

#[test]
fn validate_storage_change_first_reconcile_accepts_any() {
    assert!(validate_storage_change(None, None).is_ok());
    assert!(validate_storage_change(Some(&Storage::Ephemeral), None).is_ok());
    assert!(validate_storage_change(Some(&pc("10Gi", None)), None).is_ok());
}

#[test]
fn validate_storage_change_rejects_type_switch() {
    let observed = pvc_template("10Gi", None);
    // Observed = PersistentClaim, desired = Ephemeral.
    let err = validate_storage_change(Some(&Storage::Ephemeral), Some(&observed)).unwrap_err();
    assert!(matches!(err, PoolValidationError::StorageTypeChanged { .. }));
}

#[test]
fn validate_storage_change_rejects_class_change() {
    let observed = pvc_template("10Gi", Some("class-a"));
    let err = validate_storage_change(Some(&pc("10Gi", Some("class-b"))), Some(&observed)).unwrap_err();
    assert!(matches!(err, PoolValidationError::StorageClassChanged { .. }));
}

#[test]
fn validate_storage_change_rejects_shrink() {
    let observed = pvc_template("10Gi", None);
    let err = validate_storage_change(Some(&pc("5Gi", None)), Some(&observed)).unwrap_err();
    assert!(matches!(err, PoolValidationError::StorageShrinkNotAllowed { .. }));
}

#[test]
fn validate_storage_change_allows_grow() {
    let observed = pvc_template("10Gi", None);
    assert!(validate_storage_change(Some(&pc("20Gi", None)), Some(&observed)).is_ok());
}

#[test]
fn validate_storage_change_allows_delete_claim_flip() {
    // Same size + class; deleteClaim isn't reflected in the PVC
    // template, so the monotonic validator doesn't check it.
    let observed = pvc_template("10Gi", None);
    let mut desired = pc("10Gi", None);
    if let Storage::PersistentClaim(ref mut p) = desired {
        p.delete_claim = true;
    }
    assert!(validate_storage_change(Some(&desired), Some(&observed)).is_ok());
}

#[test]
fn validate_static_rejects_unparseable_size() {
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(pc("banana", None));
    let err = validate(&pool).unwrap_err();
    assert!(matches!(err, PoolValidationError::StorageSizeInvalid(_, _)));
}
```

- [ ] **Step 7: Verify**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator --lib controller::kafka_node_pool::tests
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: 24 existing + 7 new = 31 tests in `kafka_node_pool::tests`; clippy clean.

- [ ] **Step 8: Commit**

```bash
git add crates/operator/src/controller/kafka_node_pool.rs
git commit -m "feat(operator): validate storage size + reject type/class/shrink (slice 24, task 4)"
```

---

## Task 5 — Integration tests in `tests/reconcile_pool.rs`

**Files:**
- Modify: `crates/operator/tests/reconcile_pool.rs`
- Modify: `crates/operator/tests/shared/mod.rs` (extend the SS-GET fake body to support a `volumeClaimTemplates` injection point)

> Parallel with Task 6 (e2e workflow) — disjoint files.

- [ ] **Step 1: Extend `fake_sts_body` to accept optional volumeClaimTemplates**

Open `crates/operator/tests/shared/mod.rs`. Find the existing `fake_sts_body` (added in slice 20) — it returns a JSON value shaped like a StatefulSet for the GET mock response. Add an optional parameter that, when set, injects `volumeClaimTemplates` and `persistentVolumeClaimRetentionPolicy` into the spec. Suggested signature:

```rust
pub fn fake_sts_body(
    name: &str,
    namespace: &str,
    replicas: i32,
    ready_replicas: i32,
) -> serde_json::Value {
    fake_sts_body_with_storage(name, namespace, replicas, ready_replicas, None)
}

pub fn fake_sts_body_with_storage(
    name: &str,
    namespace: &str,
    replicas: i32,
    ready_replicas: i32,
    storage: Option<(&str, Option<&str>)>, // (size, class)
) -> serde_json::Value {
    let mut spec = serde_json::json!({
        "serviceName": format!("{name}-headless"),
        "replicas": replicas,
        "selector": { "matchLabels": {} },
        "template": { "metadata": { "labels": {} }, "spec": { "containers": [] } }
    });
    if let Some((size, class)) = storage {
        let mut pvc = serde_json::json!({
            "metadata": { "name": "data" },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": size } }
            }
        });
        if let Some(c) = class {
            pvc["spec"]["storageClassName"] = serde_json::Value::String(c.into());
        }
        spec["volumeClaimTemplates"] = serde_json::json!([pvc]);
    }
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": { "name": name, "namespace": namespace, "uid": format!("{name}-uid") },
        "spec": spec,
        "status": { "replicas": replicas, "readyReplicas": ready_replicas }
    })
}
```

(The exact existing signature of `fake_sts_body` may differ; adapt the diff to leave the slice-20/21 callers untouched. Keep the no-storage callers compiling against the old signature by retaining it as a thin shim to the new function.)

- [ ] **Step 2: Add `pool_persistent_claim_renders_volume_claim_template` test**

Append to `tests/reconcile_pool.rs`. The pool reconcile sequence for a PersistentClaim pool's happy path is:

1. GET `kafkas/<parent>` → 200 (parent Kafka body)
2. GET `statefulsets/<parent>-<pool>` → 404 (first reconcile; no live STS)
3. PATCH `statefulsets/<parent>-<pool>` (SSA) → 200 (faked STS body)
4. GET `statefulsets/<parent>-<pool>` → 200 (post-apply status read)
5. PATCH `kafkanodepools/<pool>/status` (merge) → 200

```rust
#[tokio::test]
async fn pool_persistent_claim_renders_volume_claim_template() {
    use crabka_operator::crd::{PersistentClaimSpec, Storage};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, 1)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, 1)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "10Gi".into(),
        class: Some("fast-ssd".into()),
        delete_claim: false,
    }));

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains(&format!("/statefulsets/{sts_name}")))
        .expect("STS PATCH was captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS PATCH body is JSON");

    // volumeClaimTemplates carries our data PVC at the requested size.
    let vct = body["spec"]["volumeClaimTemplates"]
        .as_array()
        .expect("volumeClaimTemplates present");
    assert_eq!(vct.len(), 1);
    assert_eq!(vct[0]["metadata"]["name"], "data");
    assert_eq!(vct[0]["spec"]["resources"]["requests"]["storage"], "10Gi");
    assert_eq!(vct[0]["spec"]["storageClassName"], "fast-ssd");

    // No emptyDir for `data` in the pod-template volumes.
    let volumes = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for v in &volumes {
        if v["name"] == "data" {
            assert!(v.get("emptyDir").is_none(), "expected no emptyDir entry for data; got {v}");
        }
    }
}
```

- [ ] **Step 3: Add `pool_storage_shrink_is_rejected` test**

```rust
#[tokio::test]
async fn pool_storage_shrink_is_rejected() {
    use crabka_operator::crd::{PersistentClaimSpec, Storage};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        // Pre-apply GET: live STS has volumeClaimTemplates with 10Gi.
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(
                200,
                &fake_sts_body_with_storage(&sts_name, ns, 1, 1, Some(("10Gi", None))),
            ),
        },
        // Validation should reject the shrink; status PATCH is the
        // only request that follows. No STS PATCH, no second STS GET.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "5Gi".into(),
        class: None,
        delete_claim: false,
    }));

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    // Assert NO STS PATCH was attempted.
    for req in &observed {
        let uri = req.uri().to_string();
        if req.method() == Method::PATCH {
            assert!(
                !uri.contains(&format!("/statefulsets/{sts_name}")),
                "shrink path must not PATCH the StatefulSet: {uri}",
            );
        }
    }
    // Status PATCH body has reason=StorageImmutable.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/status"))
        .expect("status PATCH must be captured");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Ready")
        .expect("Ready condition");
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "StorageImmutable");
}
```

- [ ] **Step 4: Verify**

```bash
cargo test -p crabka-operator --test reconcile_pool
cargo clippy -p crabka-operator --tests -- -D warnings
```

Expected: 5 existing + 2 new = 7 tests pass; clippy clean. If the existing tests broke because of the `fake_sts_body` signature change, update their call sites (the Step 1 shim should make this unnecessary).

If the slice-21 test `pool_status_ready_when_sts_ready` (or similar) needed a second STS GET to land for the post-apply read, the existing tests may now require two GET mock rules per reconcile path. Add a second GET rule for any test that asserts on the status-mirror behavior — the new pre-apply GET adds one round trip per reconcile tick.

- [ ] **Step 5: Commit**

```bash
git add crates/operator/tests/
git commit -m "test(operator): integration tests for slice 24 storage paths (slice 24, task 5)"
```

---

## Task 6 — Regenerate CRD + e2e workflow updates

**Files:**
- Modify: `deploy/crds/crabka.io_kafkanodepools.yaml` (regenerated)
- Modify: `.github/workflows/operator-e2e.yml`

> Parallel with Task 5 — disjoint files.

- [ ] **Step 1: Regenerate the CRDs**

```bash
cargo run -p crabka-operator -- gen-crds deploy/crds
git diff deploy/crds/crabka.io_kafkanodepools.yaml | head -80
```

Expected: new `storage` property under `spec` with a `oneOf`-style schema (tagged union by `type`), including `PersistentClaim` properties (`size`, `class`, `deleteClaim`).

- [ ] **Step 2: Update operator-e2e — add storage to the apply manifest**

In `.github/workflows/operator-e2e.yml`, find the existing `KafkaNodePool brokers` body inside the apply heredoc. Extend it:

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
            storage:
              type: PersistentClaim
              size: 1Gi
              deleteClaim: true
            template:
              # ...keep the existing template block from slice 20c unchanged...
```

- [ ] **Step 3: Add PVC bound smoke step**

Insert AFTER the existing `Smoke — broker binary launched in pod` step and BEFORE the slice-21 `Smoke — config change rolls broker pod` step:

```yaml
      - name: Smoke — broker bound a PersistentVolumeClaim
        run: |
          set -e
          # PVC name pattern: <volumeClaimTemplate.name>-<sts>-<ordinal>
          phase=$(kubectl get pvc data-demo-brokers-0 -n default -o jsonpath='{.status.phase}')
          [ "$phase" = "Bound" ] || { echo "::error::PVC not Bound: $phase"; kubectl describe pvc data-demo-brokers-0 -n default; exit 1; }
          cap=$(kubectl get pvc data-demo-brokers-0 -n default -o jsonpath='{.status.capacity.storage}')
          echo "PVC bound, capacity=$cap"
          instance_label=$(kubectl get pvc data-demo-brokers-0 -n default -o jsonpath='{.metadata.labels.app\.kubernetes\.io/instance}')
          [ "$instance_label" = "demo" ] || { echo "::error::PVC missing instance label: '$instance_label'"; exit 1; }
```

- [ ] **Step 4: Extend GC step's success condition to include PVCs**

Find the existing `Garbage-collection on Kafka delete` step. Replace its body to track PVCs alongside the existing labeled-resource count:

```yaml
      - name: Garbage-collection on Kafka delete
        run: |
          kubectl delete kafka demo -n default --wait=false
          for i in $(seq 1 30); do
            knp=$(kubectl get knp -n default -o name 2>/dev/null | wc -l)
            owned=$(kubectl get sts,svc,cm,secret -n default -l app.kubernetes.io/instance=demo -o name 2>/dev/null | wc -l)
            pvcs=$(kubectl get pvc -n default -l app.kubernetes.io/instance=demo -o name 2>/dev/null | wc -l)
            echo "attempt $i: knp=$knp owned=$owned pvcs=$pvcs"
            if [ "$knp" = "0" ] && [ "$owned" = "0" ] && [ "$pvcs" = "0" ]; then exit 0; fi
            sleep 2
          done
          echo "::error::owned objects not GC'd within 60s"
          kubectl get knp -n default
          kubectl get sts,svc,cm,secret -n default -l app.kubernetes.io/instance=demo
          kubectl get pvc -n default
          exit 1
```

- [ ] **Step 5: Add PVC dump to the failure-diagnostics block**

Find the existing `Collect cluster diagnostics on failure` step. Inside its `for section in` loop, add a new entry:

```
              "pvcs|kubectl get pvc -n default -o yaml" \
```

- [ ] **Step 6: YAML lint**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml')); print('YAML OK')"
```

Expected: `YAML OK`.

- [ ] **Step 7: Commit**

```bash
git add deploy/crds/ .github/workflows/operator-e2e.yml
git commit -m "ci(operator): apply PersistentClaim storage in e2e; PVC bound + GC probes (slice 24, task 6)"
```

---

## Task 7 — Final verification

**Files:** none.

> Sequential — runs last.

- [ ] **Step 1: Workspace-wide checks**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: green.

- [ ] **Step 2: Helm + drift check**

```bash
helm lint charts/crabka-operator
cargo run -p crabka-operator -- gen-crds deploy/crds
git diff --exit-code deploy/crds/
```

Expected: helm lint clean; no drift (the regen run produces no diff vs. the committed YAML).

- [ ] **Step 3: Commit, push, PR**

```bash
git push -u origin <branch>
gh pr create --title "Slice 24: Operator — persistent storage on KafkaNodePool" --body "$(cat <<'EOF'
## Summary
Adds `KafkaNodePool.spec.storage` as a tagged-union over `Ephemeral`
(default; current emptyDir) and `PersistentClaim {size, class,
deleteClaim}` (single PVC per pod via volumeClaimTemplates). Monotonic
resize allowed; type / class changes and shrinks rejected with
distinct `Ready=False` reasons.

**Spec:** `docs/superpowers/specs/2026-05-17-crabka-operator-persistent-storage-24-design.md`
**Plan:** `docs/superpowers/plans/2026-05-17-crabka-operator-persistent-storage-24.md`

## Test plan
- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] `cargo test -p crabka-operator` (existing + new tests)
- [x] `helm lint charts/crabka-operator`
- [x] CRD regen stable
- [ ] operator-e2e: PVC `data-demo-brokers-0` reaches `Bound`; cluster deletion clears the PVC alongside everything else within 60s

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Acceptance criteria recap (from spec § 8)

1. `cargo test -p crabka-operator` green.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` passes (no chart changes expected).
4. CRD regen stable for both `kafkas` and `kafkanodepools`.
5. operator-e2e (kind):
   - `Kafka demo` + `KafkaNodePool brokers` with `storage.type=PersistentClaim, size=1Gi, deleteClaim=true` becomes `Ready=True`.
   - PVC `data-demo-brokers-0` reaches `Bound` and carries `app.kubernetes.io/instance=demo`.
   - Slice-21 rolling-restart smoke still passes (same PVC binding survives).
   - `kubectl delete kafka demo` GCs the PVC alongside everything else within 60 s.
