# Tiered storage 48i — Operator PVC rendering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the `Kafka.spec.tieredStorage.persistence` plumbing so PVCs for the local-tier directory get the right retention policy and the StatefulSet's set-wide policy doesn't silently override either claim's intent.

**Architecture:** The CRD field, validation, volumeClaimTemplate rendering, and mount are already in place (slice 48i pre-work). Three small additions complete the slice: a `delete_claim: bool` field on `TieredStoragePersistence`, the policy-rendering function takes the new field into account, and a reconciler-time check rejects mismatches between the data and tier PVCs' delete-claim values (since K8s StatefulSets have no per-template retention policy).

**Tech Stack:** Rust 1.95, `kube-rs` 3.x, `k8s-openapi`, `schemars`, `serde_json::json!` macro.

**Spec:** `docs/superpowers/specs/2026-05-27-crabka-tiered-storage-operator-pvc-48i-design.md`

**Branch:** Create a new branch off `main` named `tiered-48i`.

---

## Pre-flight: branch + verify baseline

- [ ] **Step 1: Create the branch on main**

```bash
git checkout main && git pull --ff-only
git checkout -b tiered-48i
```

- [ ] **Step 2: Verify the existing 48i pre-work compiles + passes**

```bash
cargo test -p crabka-operator --lib tier_storage 2>&1 | tail -10
```

Expected: existing tests like `pod_template_emits_pvc_template_when_tier_persistence_set` pass. If anything is failing on a clean main, stop and report — that's a baseline regression, not our work.

---

## Task 1: Add `delete_claim` to `TieredStoragePersistence`

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs` around the `TieredStoragePersistence` struct (lines 156–175)

### Step 1: Read the existing struct to anchor

```bash
sed -n '156,180p' crates/operator/src/crd/kafka.rs
```

You should see:

```rust
pub struct TieredStoragePersistence {
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
}
```

### Step 2: Add the `delete_claim` field

Edit the struct in place. The new field goes after `class`:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStoragePersistence {
    /// K8s `Quantity` (e.g., `"50Gi"`, `"500Mi"`). Non-empty;
    /// resource-quantity well-formedness is validated by the
    /// Kubernetes API server at SSA time.
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`.
    /// Must match the parent `KafkaNodePool.spec.storage.deleteClaim`
    /// when both PVCs are present (K8s StatefulSets have a single
    /// set-wide retention policy with no per-template override).
    /// Validated at reconcile time; mismatch surfaces as
    /// `TieredStorageInvalid`.
    #[serde(default)]
    pub delete_claim: bool,
}
```

### Step 3: Write the round-trip test

Locate the existing `persistence_size_must_be_non_empty` test (around line 1265). Append in the same test module:

```rust
#[test]
fn persistence_delete_claim_round_trips() {
    let p = TieredStoragePersistence {
        size: "10Gi".into(),
        class: None,
        delete_claim: true,
    };
    let yaml = serde_yaml::to_string(&p).unwrap();
    assert!(yaml.contains("deleteClaim: true"));
    let back: TieredStoragePersistence = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(back, p);
}

#[test]
fn persistence_delete_claim_defaults_false() {
    let yaml = "size: 5Gi\n";
    let p: TieredStoragePersistence = serde_yaml::from_str(yaml).unwrap();
    assert!(!p.delete_claim);
}
```

### Step 4: Run the test

```bash
cargo test -p crabka-operator --lib persistence_delete_claim 2>&1 | tail -10
```

Expected: 2 passed.

Existing tests at the same location (e.g. `persistence_requires_local_kind`, `persistence_size_must_be_non_empty`) may need updating — they construct `TieredStoragePersistence` literal values. The new `delete_claim` field has a `Default`, so plain `Default::default()` constructions still work, but explicit struct-literal constructions need the new field. Grep:

```bash
grep -n "TieredStoragePersistence {" crates/operator/ -r 2>&1 | head
```

For each hit, add `delete_claim: false,` so the literal compiles.

### Step 5: Run the broader test suite to confirm nothing regressed

```bash
cargo test -p crabka-operator --lib 2>&1 | tail -10
```

Expected: all passing.

### Step 6: Clippy

```bash
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail -5
```

Expected: clean.

### Step 7: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/operator/src/crd/kafka.rs
# Also stage any test files where TieredStoragePersistence literals were updated:
# git ... add crates/operator/...
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "operator(crd): add deleteClaim to TieredStoragePersistence"
```

---

## Task 2: Validate `delete_claim` consistency with the pool's data PVC

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs` (the per-pool render function — around line 689 where `render_statefulset` starts)

### Step 1: Find the early reconcile-time validation site

Read the top of `render_statefulset` (around line 689–760). Look for where other `ReconcileError`s are returned early. If there isn't one yet, add the check immediately after the line that resolves `tiered_storage` (search for `let tier_storage_persistence = tiered_storage.and_then(...)` near line 838).

### Step 2: Write the failing test

In the existing test module of `kafka_node_pool.rs` (search for `// ── Slice 48i: tier-storage PVC tests ────────`, around line 2921), append:

```rust
#[test]
fn tier_persistence_delete_claim_mismatch_fails_validation() {
    use crate::crd::kafka::{TieredStorage, TieredStorageType, TieredStoragePersistence};
    use crate::crd::kafka_node_pool::{PersistentClaimSpec, Storage};

    let mut parent = parent_fixture("demo");
    parent.spec.tiered_storage = Some(TieredStorage {
        kind: TieredStorageType::Local,
        s3: None,
        metadata_manager: None,
        persistence: Some(TieredStoragePersistence {
            size: "20Gi".into(),
            class: None,
            delete_claim: true,
        }),
    });
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "100Gi".into(),
        class: None,
        delete_claim: false,
    }));

    let err = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
        .err()
        .expect("must reject deleteClaim mismatch");
    let msg = format!("{err:?}");
    assert!(msg.contains("TieredStorageInvalid"), "got: {msg}");
    assert!(msg.contains("deleteClaim"), "got: {msg}");
}

#[test]
fn tier_persistence_delete_claim_matching_pool_passes() {
    use crate::crd::kafka::{TieredStorage, TieredStorageType, TieredStoragePersistence};
    use crate::crd::kafka_node_pool::{PersistentClaimSpec, Storage};

    let mut parent = parent_fixture("demo");
    parent.spec.tiered_storage = Some(TieredStorage {
        kind: TieredStorageType::Local,
        s3: None,
        metadata_manager: None,
        persistence: Some(TieredStoragePersistence {
            size: "20Gi".into(),
            class: None,
            delete_claim: false,
        }),
    });
    let mut pool = pool_fixture("brokers", "demo", 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "100Gi".into(),
        class: None,
        delete_claim: false,
    }));

    // Should succeed; we only care that it doesn't error.
    let _sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
        .expect("matching deleteClaim must pass");
}

#[test]
fn tier_persistence_with_ephemeral_pool_storage_passes() {
    use crate::crd::kafka::{TieredStorage, TieredStorageType, TieredStoragePersistence};

    let mut parent = parent_fixture("demo");
    parent.spec.tiered_storage = Some(TieredStorage {
        kind: TieredStorageType::Local,
        s3: None,
        metadata_manager: None,
        persistence: Some(TieredStoragePersistence {
            size: "20Gi".into(),
            class: None,
            delete_claim: true,
        }),
    });
    // pool.spec.storage stays None (ephemeral); no data PVC to collide with
    let pool = pool_fixture("brokers", "demo", 1);

    let _sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
        .expect("ephemeral pool + tier persistence must pass regardless of tier deleteClaim");
}
```

Run:

```bash
cargo test -p crabka-operator --lib tier_persistence_delete_claim 2>&1 | tail -10
```

Expected: compile error or 2 of 3 failing (the matching + ephemeral cases would already pass because no validation exists yet, but the mismatch test panics on the unwrap).

### Step 3: Add the validation in `render_statefulset`

In `crates/operator/src/controller/kafka_node_pool.rs`, after the line that binds `tier_storage_persistence`:

```rust
let tier_storage_persistence = tiered_storage.and_then(|t| t.persistence.as_ref());
```

Add:

```rust
// Slice 48i: K8s StatefulSets have a single set-wide PVC retention
// policy; per-template overrides don't exist. When the pool has both
// a data PVC and a tier PVC, their `delete_claim` flags must match
// (otherwise we'd silently pick one and lose data in a way the user
// didn't intend). Pool-Ephemeral skips this check — there's no data
// PVC to collide with.
if let Some(tp) = tier_storage_persistence {
    let pool_data_delete_claim = match pool.spec.storage.as_ref() {
        Some(Storage::PersistentClaim(pc)) => Some(pc.delete_claim),
        Some(Storage::Jbod(j))             => Some(j.delete_claim),
        _ => None,
    };
    if let Some(dc) = pool_data_delete_claim
        && dc != tp.delete_claim
    {
        return Err(ReconcileError::TieredStorageInvalid(format!(
            "tiered storage persistence.deleteClaim={} but pool '{}' storage.deleteClaim={}; \
             K8s StatefulSets have a single set-wide PVC retention policy — these must match",
            tp.delete_claim, pool_name, dc,
        )));
    }
}
```

Verify `Storage` and `ReconcileError` are already in scope (look for the existing `use` block at the top of the file); if not, add:

```rust
use crate::crd::kafka_node_pool::Storage;
use crate::controller::common::ReconcileError;
```

### Step 4: Run the tests

```bash
cargo test -p crabka-operator --lib tier_persistence 2>&1 | tail -15
```

Expected: all three new tests pass plus the existing `pod_template_emits_pvc_template_when_tier_persistence_set` still passes.

If the existing test fails because its `TieredStoragePersistence` construction now needs `delete_claim`, update it (add `delete_claim: false,` to the literal).

### Step 5: Clippy

```bash
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail -5
```

Expected: clean.

### Step 6: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/operator/src/controller/kafka_node_pool.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "operator: validate tier-storage deleteClaim matches pool data PVC"
```

---

## Task 3: Extend `render_pvc_retention_policy` to consider the tier PVC

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs:657–670` (the function)
- Modify: `crates/operator/src/controller/kafka_node_pool.rs:848` (the call site)

### Step 1: Write the failing test

In the same Slice 48i test block, append:

```rust
#[test]
fn ephemeral_pool_with_tier_persistence_emits_retention_policy() {
    use crate::crd::kafka::{TieredStorage, TieredStorageType, TieredStoragePersistence};

    let mut parent = parent_fixture("demo");
    parent.spec.tiered_storage = Some(TieredStorage {
        kind: TieredStorageType::Local,
        s3: None,
        metadata_manager: None,
        persistence: Some(TieredStoragePersistence {
            size: "20Gi".into(),
            class: None,
            delete_claim: true,
        }),
    });
    // pool.spec.storage = None  → ephemeral; no data PVC
    let pool = pool_fixture("brokers", "demo", 1);
    let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
    let policy = sts
        .spec
        .as_ref()
        .unwrap()
        .persistent_volume_claim_retention_policy
        .as_ref()
        .expect("policy must exist when tier PVC is present");
    assert_eq!(policy.when_deleted.as_deref(), Some("Delete"),
        "delete_claim=true should map to whenDeleted=Delete");
    assert_eq!(policy.when_scaled.as_deref(), Some("Retain"));
}

#[test]
fn ephemeral_pool_without_tier_persistence_emits_no_retention_policy() {
    let parent = parent_fixture("demo");
    let pool = pool_fixture("brokers", "demo", 1);
    let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
    assert!(
        sts.spec
            .as_ref()
            .unwrap()
            .persistent_volume_claim_retention_policy
            .is_none(),
        "no PVCs ⇒ no retention policy"
    );
}
```

Run:

```bash
cargo test -p crabka-operator --lib retention_policy 2>&1 | tail -10
```

Expected: `ephemeral_pool_with_tier_persistence_emits_retention_policy` FAILS (the existing function returns None for ephemeral pools). The "no-policy" test passes (current behavior).

### Step 2: Extend `render_pvc_retention_policy`

Replace the existing function at line 657:

```rust
/// Build the `StatefulSet`'s `persistentVolumeClaimRetentionPolicy`
/// block when any PVC is in play. Returns `None` only when neither
/// the pool's data storage nor the tier-storage cache is a PVC.
///
/// A StatefulSet's retention policy applies set-wide to every
/// `volumeClaimTemplate`. Validation upstream (Task 2) ensures that
/// when both data and tier PVCs exist, their `delete_claim` flags
/// match — so we can pick the pool's value when present and the tier
/// value otherwise.
fn render_pvc_retention_policy(
    storage: Option<&Storage>,
    tier_persistence: Option<&crate::crd::kafka::TieredStoragePersistence>,
) -> Option<serde_json::Value> {
    let delete_claim = match storage {
        Some(Storage::PersistentClaim(pc)) => pc.delete_claim,
        Some(Storage::Jbod(j))             => j.delete_claim,
        _ => match tier_persistence {
            Some(p) => p.delete_claim,
            None    => return None,
        },
    };
    Some(json!({
        "whenDeleted": if delete_claim { "Delete" } else { "Retain" },
        "whenScaled": "Retain",
    }))
}
```

### Step 3: Update the call site

The existing call at line 848 needs the new arg. Find it:

```bash
grep -n "render_pvc_retention_policy(" crates/operator/src/controller/kafka_node_pool.rs
```

Update from:

```rust
let retention_policy = render_pvc_retention_policy(pool.spec.storage.as_ref());
```

to:

```rust
let retention_policy = render_pvc_retention_policy(
    pool.spec.storage.as_ref(),
    tier_storage_persistence,
);
```

`tier_storage_persistence` is already bound earlier in the function (verified in Task 2).

### Step 4: Run the tests

```bash
cargo test -p crabka-operator --lib retention_policy 2>&1 | tail -10
cargo test -p crabka-operator --lib tier 2>&1 | tail -15
```

Expected: both retention-policy tests pass; all tier_persistence_* tests still pass.

### Step 5: Run the full operator suite to catch ripples

```bash
cargo test -p crabka-operator 2>&1 | tail -10
```

Expected: no new failures. Any pre-existing tests asserting the policy is `None` for ephemeral pools (without tier persistence) still hold.

### Step 6: Clippy + fmt

```bash
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 7: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/operator/src/controller/kafka_node_pool.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "operator: render PVC retention policy when only the tier-storage PVC is present"
```

---

## Final verification

- [ ] **Step 1: Workspace build + test**

```bash
cargo build --workspace 2>&1 | tail -3
cargo test --workspace --lib 2>&1 | grep -E "test result|FAILED" | tail -20
```

Expected: clean build, no test regressions.

- [ ] **Step 2: Workspace clippy + fmt**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 3: Open PR**

```bash
git push -u origin tiered-48i
gh pr create --title "Slice 48i: operator PVC rendering for tiered-storage local-tier directory" --body "$(cat <<'EOF'
## Summary

Finishes the `Kafka.spec.tieredStorage.persistence` plumbing landed in earlier 48i pre-work. The CRD field, volume rendering, mount, and a passing unit test were already in place; this PR adds the three pieces that were missing:

- `delete_claim: bool` field on `TieredStoragePersistence`.
- `render_pvc_retention_policy` now emits a policy when the pool has no data PVC but the tier-storage PVC is present.
- Reconciler validation rejects `Kafka` CRs where the data and tier PVC `delete_claim` values disagree (K8s StatefulSets have a single set-wide retention policy with no per-template override).

With slices 48h (topic-based RLMM) and 48i, tier segment bytes + tier metadata both survive pod restarts.

Spec: `docs/superpowers/specs/2026-05-27-crabka-tiered-storage-operator-pvc-48i-design.md`
Plan: `docs/superpowers/plans/2026-05-27-crabka-tiered-storage-operator-pvc-48i.md`

## Test plan

- [x] `cargo build --workspace`
- [x] `cargo test --workspace --lib`
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] `cargo fmt --check`
- [ ] CI operator-e2e job (PVC binding is best validated end-to-end against the kind cluster)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.
