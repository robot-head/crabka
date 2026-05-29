# Broker runtime `metadata.version` enforcement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the broker enforce `metadata.version` at runtime — a Kafka-faithful level table, a finalized MV bootstrapped at format time, a fail-fast range guard, a downgrade-safety floor, and per-RPC admission gates.

**Architecture:** The canonical `MetadataVersion` table lives in `crabka_metadata` (the only crate every consumer — broker, raft, operator, cli — depends on). `crabka_metadata` also exposes image accessors for the finalized level and the live-state floor. The raft state machine fail-fast-aborts on an out-of-range committed level; the `UpdateFeatures` handler refuses downgrades below the floor; SCRAM/delegation-token handlers reject RPCs below their introduction level; `crabka format --release-version` seeds the bootstrap `V1FeatureLevel`; the operator passes its resolved version into the format init container and gains a `MetadataVersionTooLow` reason.

**Tech Stack:** Rust (workspace crates `crabka-metadata`, `crabka-raft`, `crabka-broker`, `crabka-cli`, `crabka-operator`), `cargo test`/`cargo clippy`/`cargo fmt`, Docker `jvm_acceptance` suite.

**Design spec:** `docs/superpowers/specs/2026-05-29-crabka-metadata-version-enforcement-design.md`

---

## Refinements to the spec (locked during planning)

- **Table location:** the spec said `crates/broker/src/features.rs`. The runtime guard lives in `crates/raft/src/state_machine.rs`, and `raft` cannot depend on `broker`. The dependency graph (`broker`, `raft`, `operator`, `cli` all → `metadata`; `raft` ⊄ `broker`) makes `crabka_metadata` the only correct home. The canonical table moves to a new `crates/metadata/src/metadata_version.rs`; `broker/features.rs` becomes a thin re-export so its existing `METADATA_VERSION_MAX` / `lookup` API surface keeps working for `api_versions.rs`.
- **Operator handoff site:** the resolved `metadata.version` is injected into the `INIT_SCRIPT` (`crates/operator/src/controller/kafka_node_pool.rs:179`) as `crabka format --release-version "$CRABKA_METADATA_VERSION"`, with `CRABKA_METADATA_VERSION` wired as an init-container env var alongside `CRABKA_CLUSTER_ID`. `common.rs` keeps injecting `metadata.version` into `[server_properties]` (now advisory/descriptive parity).

## Exact Kafka 4.0 `MetadataVersion` levels

Mirror these levels + `X.Y-IVn` names exactly (upstream `org.apache.kafka.server.common.MetadataVersion`). **Task 1 begins by verifying this table** against `apache/kafka:4.0.0`.

| level | name | short | level | name | short |
|---|---|---|---|---|---|
| 7  | 3.3-IV3 | 3.3 | 17 | 3.7-IV2 | 3.7 |
| 8  | 3.4-IV0 | 3.4 | 18 | 3.7-IV3 | 3.7 |
| 9  | 3.5-IV0 | 3.5 | 19 | 3.7-IV4 | 3.7 |
| 10 | 3.5-IV1 | 3.5 | 20 | 3.8-IV0 | 3.8 |
| 11 | 3.5-IV2 | 3.5 | 21 | 3.9-IV0 | 3.9 |
| 12 | 3.6-IV0 | 3.6 | 22 | 4.0-IV0 | 4.0 |
| 13 | 3.6-IV1 | 3.6 | 23 | 4.0-IV1 | 4.0 |
| 14 | 3.6-IV2 | 3.6 | 24 | 4.0-IV2 | 4.0 |
| 15 | 3.7-IV0 | 3.7 | 25 | 4.0-IV3 | 4.0 |
| 16 | 3.7-IV1 | 3.7 |   |   |   |

- `METADATA_VERSION_MIN = 7` (3.3-IV3), `METADATA_VERSION_MAX = 25` (4.0-IV3).
- Feature gate levels: SCRAM = `11` (3.5-IV2); delegation tokens = `14` (3.6-IV2).

---

## Batch plan (parallel batches by non-overlapping file set)

- **Batch 1 (foundation, runs alone):** Task 1 — `crates/metadata/src/metadata_version.rs` + `lib.rs`.
- **Batch 2 (parallel; each depends only on Batch 1):** Task 2 — `image.rs`; Task 3 — `cli/format.rs`; Task 4 — `operator/version.rs`.
- **Batch 3 (parallel; depend on Batches 1–2):** Task 5 — `raft/state_machine.rs`; Task 6 — `broker/features.rs`; Task 7 — `broker/handlers/update_features.rs`; Task 8 — `operator/controller/kafka_node_pool.rs`.
- **Batch 4 (parallel; per-RPC gates):** Task 9 — `handlers/alter_user_scram_credentials.rs`; Task 10 — the three `*_delegation_token.rs` handlers.
- **Batch 5 (finalize, runs alone):** Task 11 — integration tests + STATUS.md + jvm_acceptance.

Within a batch, file sets do not overlap, so dispatch all tasks concurrently. Tasks 6 and 7 both live in `crates/broker` but touch different files (`features.rs` vs `handlers/update_features.rs`) and neither calls the other's new code, so they are conflict-free.

---

## Batch 1

### Task 1: `MetadataVersion` table in `crabka_metadata`

**Files:**
- Create: `crates/metadata/src/metadata_version.rs`
- Modify: `crates/metadata/src/lib.rs` (add `pub mod metadata_version;`)
- Test: inline `#[cfg(test)]` in `metadata_version.rs`

- [ ] **Step 1: Verify the level table against cp-kafka 4.0**

Run:
```bash
docker run --rm apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-features.sh --help >/dev/null 2>&1 || true
# Authoritative source: the MetadataVersion enum. Confirm MIN=7 (3.3-IV3),
# MAX=25 (4.0-IV3), SCRAM=11 (3.5-IV2), delegation tokens=14 (3.6-IV2)
# against org.apache.kafka.server.common.MetadataVersion for 4.0.0.
```
Expected: the level/name pairs in the table above match upstream 4.0.0. If any differ, update the `TABLE` constant and the gate-level constants below before proceeding.

- [ ] **Step 2: Write the failing tests**

Add to `crates/metadata/src/metadata_version.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_max_levels() {
        assert_eq!(METADATA_VERSION_MIN, 7);
        assert_eq!(METADATA_VERSION_MAX, 25);
    }

    #[test]
    fn from_feature_level_known_and_unknown() {
        assert_eq!(from_feature_level(7).unwrap().short(), "3.3");
        assert_eq!(from_feature_level(25).unwrap().ivn(), "4.0-IV3");
        assert!(from_feature_level(6).is_none());
        assert!(from_feature_level(26).is_none());
    }

    #[test]
    fn from_version_string_exact_ivn() {
        assert_eq!(from_version_string("3.5-IV2").unwrap().feature_level(), 11);
        assert_eq!(from_version_string("4.0-IV3").unwrap().feature_level(), 25);
        assert!(from_version_string("3.5-IV9").is_none());
    }

    #[test]
    fn from_version_string_short_picks_highest_in_minor() {
        // "3.7" spans levels 15..=19; the short form resolves to the
        // highest level within that minor (matches MetadataVersion).
        assert_eq!(from_version_string("3.7").unwrap().feature_level(), 19);
        assert_eq!(from_version_string("4.0").unwrap().feature_level(), 25);
        assert!(from_version_string("2.8").is_none());
    }

    #[test]
    fn in_supported_range_predicate() {
        assert!(is_supported_level(7));
        assert!(is_supported_level(25));
        assert!(!is_supported_level(6));
        assert!(!is_supported_level(26));
    }

    #[test]
    fn gate_level_constants() {
        assert_eq!(SCRAM_MIN_LEVEL, 11);
        assert_eq!(DELEGATION_TOKEN_MIN_LEVEL, 14);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p crabka-metadata metadata_version`
Expected: FAIL — `metadata_version` module / symbols not found.

- [ ] **Step 4: Implement the module**

Create `crates/metadata/src/metadata_version.rs`:
```rust
//! KIP-778 `metadata.version` feature-level model. The canonical
//! string<->integer-level table, mirrored byte-for-byte from upstream
//! Kafka's `MetadataVersion` enum over the range Crabka advertises
//! (`[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`). JVM clients call
//! `MetadataVersion.fromFeatureLevel(N)` and throw on any level their
//! enum doesn't know, so the levels and `X.Y-IVn` names here MUST match
//! upstream exactly. Verify against the cp-kafka 4.0 enum before editing.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub const METADATA_VERSION_FEATURE: &str = "metadata.version";

/// Minimum supported level: `3.3-IV3` (KRaft GA) — the floor real Kafka
/// 4.0 supports.
pub const METADATA_VERSION_MIN: i16 = 7;
/// Maximum supported level: `4.0-IV3`.
pub const METADATA_VERSION_MAX: i16 = 25;

/// Level at which KRaft gained SCRAM credentials (`3.5-IV2`).
pub const SCRAM_MIN_LEVEL: i16 = 11;
/// Level at which KRaft gained delegation tokens (`3.6-IV2`).
pub const DELEGATION_TOKEN_MIN_LEVEL: i16 = 14;

/// One `metadata.version` level: its integer feature level, canonical
/// `X.Y-IVn` name, and short `X.Y` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataVersion {
    level: i16,
    ivn: &'static str,
    short: &'static str,
}

impl MetadataVersion {
    #[must_use]
    pub fn feature_level(self) -> i16 {
        self.level
    }
    #[must_use]
    pub fn ivn(self) -> &'static str {
        self.ivn
    }
    #[must_use]
    pub fn short(self) -> &'static str {
        self.short
    }
}

const TABLE: &[MetadataVersion] = &[
    MetadataVersion { level: 7,  ivn: "3.3-IV3", short: "3.3" },
    MetadataVersion { level: 8,  ivn: "3.4-IV0", short: "3.4" },
    MetadataVersion { level: 9,  ivn: "3.5-IV0", short: "3.5" },
    MetadataVersion { level: 10, ivn: "3.5-IV1", short: "3.5" },
    MetadataVersion { level: 11, ivn: "3.5-IV2", short: "3.5" },
    MetadataVersion { level: 12, ivn: "3.6-IV0", short: "3.6" },
    MetadataVersion { level: 13, ivn: "3.6-IV1", short: "3.6" },
    MetadataVersion { level: 14, ivn: "3.6-IV2", short: "3.6" },
    MetadataVersion { level: 15, ivn: "3.7-IV0", short: "3.7" },
    MetadataVersion { level: 16, ivn: "3.7-IV1", short: "3.7" },
    MetadataVersion { level: 17, ivn: "3.7-IV2", short: "3.7" },
    MetadataVersion { level: 18, ivn: "3.7-IV3", short: "3.7" },
    MetadataVersion { level: 19, ivn: "3.7-IV4", short: "3.7" },
    MetadataVersion { level: 20, ivn: "3.8-IV0", short: "3.8" },
    MetadataVersion { level: 21, ivn: "3.9-IV0", short: "3.9" },
    MetadataVersion { level: 22, ivn: "4.0-IV0", short: "4.0" },
    MetadataVersion { level: 23, ivn: "4.0-IV1", short: "4.0" },
    MetadataVersion { level: 24, ivn: "4.0-IV2", short: "4.0" },
    MetadataVersion { level: 25, ivn: "4.0-IV3", short: "4.0" },
];

/// Look up a level by integer feature level. `None` if outside the
/// supported table.
#[must_use]
pub fn from_feature_level(level: i16) -> Option<MetadataVersion> {
    TABLE.iter().copied().find(|m| m.level == level)
}

/// Resolve a version string to a level. Accepts both the exact `X.Y-IVn`
/// form and the short `X.Y` form; the short form resolves to the highest
/// level within that minor (matching `MetadataVersion.fromVersionString`).
#[must_use]
pub fn from_version_string(s: &str) -> Option<MetadataVersion> {
    let s = s.trim();
    if s.contains('-') {
        return TABLE.iter().copied().find(|m| m.ivn == s);
    }
    TABLE
        .iter()
        .copied()
        .filter(|m| m.short == s)
        .max_by_key(|m| m.level)
}

/// True if `level` is within `[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`.
#[must_use]
pub fn is_supported_level(level: i16) -> bool {
    (METADATA_VERSION_MIN..=METADATA_VERSION_MAX).contains(&level)
}
```

Add to `crates/metadata/src/lib.rs` (alongside the other `pub mod` lines):
```rust
pub mod metadata_version;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-metadata metadata_version`
Expected: PASS (6 tests).

- [ ] **Step 6: fmt + clippy + commit**

Run: `cargo fmt -p crabka-metadata && cargo clippy -p crabka-metadata -- -D warnings`
Expected: clean.
```bash
git add crates/metadata/src/metadata_version.rs crates/metadata/src/lib.rs
git commit -m "feat(metadata): Kafka-faithful metadata.version level table"
```

---

## Batch 2

### Task 2: `MetadataImage` finalized-level + floor accessors

**Files:**
- Modify: `crates/metadata/src/image.rs` (add two methods + tests)
- Test: inline `#[cfg(test)]` in `image.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/metadata/src/image.rs`:
```rust
#[test]
fn finalized_metadata_version_reads_feature_map() {
    use crate::records::FeatureLevelRecord;
    let mut m = img();
    assert_eq!(m.finalized_metadata_version(), None);
    m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: "metadata.version".into(),
        level: 19,
    }));
    assert_eq!(m.finalized_metadata_version(), Some(19));
}

#[test]
fn min_required_metadata_version_baseline_is_min() {
    use crate::metadata_version::METADATA_VERSION_MIN;
    let m = img();
    assert_eq!(m.min_required_metadata_version(), METADATA_VERSION_MIN);
}

#[test]
fn min_required_metadata_version_rises_with_scram_and_tokens() {
    use crate::metadata_version::{SCRAM_MIN_LEVEL, DELEGATION_TOKEN_MIN_LEVEL};
    use crabka_security::{KafkaPrincipal, SaslMechanism};
    let mut m = img();
    m.apply(&MetadataRecord::V1ScramCredential(crate::records::ScramCredentialRecord {
        user: "alice".into(),
        mechanism: SaslMechanism::ScramSha512,
        salt: vec![1; 16],
        stored_key: vec![2; 64],
        server_key: vec![3; 64],
        iterations: 4096,
    }));
    assert_eq!(m.min_required_metadata_version(), SCRAM_MIN_LEVEL);
    m.apply(&MetadataRecord::V1DelegationToken(crate::records::DelegationTokenRecord {
        token_id: "t1".into(),
        owner: KafkaPrincipal { principal_type: "User".into(), name: "alice".into() },
        hmac: vec![0x42; 32],
        issue_timestamp_ms: 1,
        expiry_timestamp_ms: 5,
        max_timestamp_ms: 10,
        renewers: vec![],
    }));
    assert_eq!(m.min_required_metadata_version(), DELEGATION_TOKEN_MIN_LEVEL);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-metadata image::tests::min_required`
Expected: FAIL — methods not found.

- [ ] **Step 3: Implement the accessors**

Add to `impl MetadataImage` in `crates/metadata/src/image.rs` (near `finalized_features`):
```rust
/// The finalized `metadata.version` level, or `None` if no
/// `V1FeatureLevel` for `metadata.version` has been applied
/// (a pre-bootstrap / legacy image — `MetadataVersion.UNKNOWN`).
#[must_use]
pub fn finalized_metadata_version(&self) -> Option<i16> {
    self.feature_levels
        .get(crate::metadata_version::METADATA_VERSION_FEATURE)
        .copied()
}

/// The minimum `metadata.version` level the live image requires: the
/// floor a downgrade must not drop below. Rises with feature-gated
/// state present in the image (KRaft SCRAM creds, delegation tokens).
/// Baseline is `METADATA_VERSION_MIN`.
#[must_use]
pub fn min_required_metadata_version(&self) -> i16 {
    use crate::metadata_version::{
        DELEGATION_TOKEN_MIN_LEVEL, METADATA_VERSION_MIN, SCRAM_MIN_LEVEL,
    };
    let mut floor = METADATA_VERSION_MIN;
    if !self.scram_credentials.is_empty() {
        floor = floor.max(SCRAM_MIN_LEVEL);
    }
    if !self.delegation_tokens.is_empty() {
        floor = floor.max(DELEGATION_TOKEN_MIN_LEVEL);
    }
    floor
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-metadata image::tests`
Expected: PASS.

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-metadata && cargo clippy -p crabka-metadata -- -D warnings`
```bash
git add crates/metadata/src/image.rs
git commit -m "feat(metadata): finalized + min-required metadata.version accessors"
```

### Task 3: `crabka format --release-version` bootstrap record

**Files:**
- Modify: `crates/cli/src/format.rs` (add arg, emit record, tests)
- Modify: `crates/cli/Cargo.toml` only if `crabka-metadata` is not already a dep (it is — no change expected)
- Test: inline `#[cfg(test)]` in `format.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/cli/src/format.rs`:
```rust
#[test]
fn release_version_maps_to_feature_level() {
    // The CLI maps a release string to the metadata.version feature level
    // via the shared table and emits a bootstrap V1FeatureLevel record.
    assert_eq!(resolve_release_level("4.0").unwrap(), 25);
    assert_eq!(resolve_release_level("3.7-IV4").unwrap(), 19);
    assert!(resolve_release_level("2.8").is_err());      // below MIN / unknown
    assert!(resolve_release_level("9.9-IV0").is_err());  // unknown
}

#[test]
fn default_release_level_is_max() {
    assert_eq!(resolve_release_level("4.0").unwrap(), crabka_metadata::metadata_version::METADATA_VERSION_MAX);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-cli format::tests::release_version`
Expected: FAIL — `resolve_release_level` not found.

- [ ] **Step 3: Implement the arg + resolver + record emission**

Add the field to `FormatArgs` (after `cluster_id`):
```rust
    /// Bootstrap `metadata.version` (KIP-778), e.g. `4.0` or `4.0-IV3`.
    /// Defaults to the broker's maximum supported level when omitted.
    #[arg(long)]
    release_version: Option<String>,
```

Add the resolver helper (near `parse_scram_spec`):
```rust
/// Map a release string to a supported `metadata.version` feature level,
/// erroring if it is unknown or outside `[MIN, MAX]`.
fn resolve_release_level(s: &str) -> Result<i16, String> {
    let mv = crabka_metadata::metadata_version::from_version_string(s)
        .ok_or_else(|| format!("unknown metadata.version {s:?}"))?;
    let level = mv.feature_level();
    if !crabka_metadata::metadata_version::is_supported_level(level) {
        return Err(format!(
            "metadata.version {s:?} (level {level}) is outside the supported range"
        ));
    }
    Ok(level)
}
```

In `run`, after the `V1KRaftVersion` / `V1Voters` seed records are pushed and before the `--add-scram` loop, emit the feature-level record:
```rust
    // KIP-778 bootstrap: every formatted cluster finalizes a real
    // metadata.version so the image never sits at MetadataVersion.UNKNOWN.
    let release = args
        .release_version
        .as_deref()
        .map(resolve_release_level)
        .transpose();
    let release_level = match release {
        Ok(Some(level)) => level,
        Ok(None) => crabka_metadata::metadata_version::METADATA_VERSION_MAX,
        Err(e) => {
            eprintln!("crabka format: {e}");
            return EXIT_BOOTSTRAP_FAIL;
        }
    };
    records.push(MetadataRecord::V1FeatureLevel(
        crabka_metadata::FeatureLevelRecord {
            name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.to_string(),
            level: release_level,
        },
    ));
```

Add `FeatureLevelRecord` to the `crabka_metadata` import list at the top of the file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-cli format`
Expected: PASS.

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-cli && cargo clippy -p crabka-cli -- -D warnings`
```bash
git add crates/cli/src/format.rs
git commit -m "feat(cli): crabka format --release-version seeds metadata.version"
```

### Task 4: operator `MetadataVersionTooLow` + range bounds

**Files:**
- Modify: `crates/operator/src/version.rs` (add reason, bound check, tests)
- Test: inline `#[cfg(test)]` in `version.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/operator/src/version.rs`:
```rust
#[test]
fn resolved_below_broker_min_is_too_low() {
    // 3.2 maps below the broker's metadata.version floor (3.3-IV3).
    let out = evaluate("3.2.0", None, None);
    assert!(matches!(
        out,
        VersionOutcome::Invalid { reason: VersionReason::MetadataVersionTooLow, .. }
    ));
}

#[test]
fn resolved_at_or_above_min_is_valid() {
    let out = evaluate("3.7.0", None, None);
    assert!(matches!(out, VersionOutcome::Valid { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-operator version::tests::resolved_below`
Expected: FAIL — `MetadataVersionTooLow` variant not found.

- [ ] **Step 3: Implement the reason + bound check**

Add the variant to `VersionReason`:
```rust
    /// The resolved metadata version is below the broker's supported floor.
    MetadataVersionTooLow,
```

In `evaluate`, after the `MetadataVersionTooHigh` check and before the finalized-downgrade check, add:
```rust
    // The broker aborts on a finalized metadata.version below its
    // supported floor (3.3-IV3). Refuse to inject one.
    if let Some(mv) = crabka_metadata::metadata_version::from_version_string(&resolved.short()) {
        if mv.feature_level() < crabka_metadata::metadata_version::METADATA_VERSION_MIN {
            return VersionOutcome::Invalid {
                reason: VersionReason::MetadataVersionTooLow,
                message: format!(
                    "metadata.version {} is below the broker's supported floor (3.3-IV3)",
                    resolved.short()
                ),
            };
        }
    } else {
        return VersionOutcome::Invalid {
            reason: VersionReason::MetadataVersionTooLow,
            message: format!(
                "metadata.version {} is not a supported level",
                resolved.short()
            ),
        };
    }
```

Confirm `crabka-metadata` is a dependency of `crates/operator/Cargo.toml` (it is — verified in planning).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-operator version`
Expected: PASS (existing 17 + 2 new).

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-operator && cargo clippy -p crabka-operator -- -D warnings`
```bash
git add crates/operator/src/version.rs
git commit -m "feat(operator): reject metadata.version below broker floor"
```

---

## Batch 3

### Task 5: raft state-machine range guard (fail-fast)

**Files:**
- Modify: `crates/raft/src/state_machine.rs` (guard in `apply_entry` + after `install_snapshot`; tests)
- Test: inline `#[cfg(test)]` in `state_machine.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/raft/src/state_machine.rs`:
```rust
#[test]
fn guard_rejects_out_of_range_finalized_level() {
    // Pure predicate: present-and-out-of-range trips; missing or in-range
    // does not.
    assert!(metadata_version_out_of_range(Some(6)));   // below MIN
    assert!(metadata_version_out_of_range(Some(26)));  // above MAX
    assert!(!metadata_version_out_of_range(Some(7)));
    assert!(!metadata_version_out_of_range(Some(25)));
    assert!(!metadata_version_out_of_range(None));     // UNKNOWN is allowed
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-raft state_machine::tests::guard_rejects`
Expected: FAIL — `metadata_version_out_of_range` not found.

- [ ] **Step 3: Implement the predicate + wire the guard**

Add the free function near the top of `crates/raft/src/state_machine.rs` (module scope):
```rust
/// True when a *present* finalized `metadata.version` is outside the
/// binary's supported range. A missing level (`None`) is permitted — a
/// pre-bootstrap / legacy image advertises `MetadataVersion.UNKNOWN`.
fn metadata_version_out_of_range(finalized: Option<i16>) -> bool {
    finalized.is_some_and(|level| !crabka_metadata::metadata_version::is_supported_level(level))
}

/// Abort the process if the image's finalized `metadata.version` is
/// outside `[MIN, MAX]`. Apply is infallible (the committed record cannot
/// be rejected), so an out-of-range level — applied via a snapshot from a
/// newer binary or a record finalized by a newer controller — means this
/// binary cannot safely interpret the metadata log. Fail loud and fast;
/// the operator's `binary >= finalized` guard is what prevents this on a
/// correctly-run cluster.
fn guard_metadata_version(image: &crabka_metadata::MetadataImage) {
    let finalized = image.finalized_metadata_version();
    if metadata_version_out_of_range(finalized) {
        tracing::error!(
            finalized = ?finalized,
            min = crabka_metadata::metadata_version::METADATA_VERSION_MIN,
            max = crabka_metadata::metadata_version::METADATA_VERSION_MAX,
            "finalized metadata.version is outside this binary's supported range; aborting"
        );
        std::process::abort();
    }
}
```

In `apply_entry`, immediately after `let _ = self.image.send_replace(Arc::new(next));`, add:
```rust
        guard_metadata_version(self.image.borrow().as_ref());
```

In `install_snapshot`, after the image is rebuilt and stored (immediately after the image `send_replace`/assignment that publishes the installed snapshot's image), add the same call:
```rust
        guard_metadata_version(self.image.borrow().as_ref());
```
(Locate the exact line where `install_snapshot` publishes the rebuilt image — it mirrors `apply_entry`'s `send_replace`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-raft state_machine`
Expected: PASS.

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-raft && cargo clippy -p crabka-raft -- -D warnings`
```bash
git add crates/raft/src/state_machine.rs
git commit -m "feat(raft): fail-fast on out-of-range finalized metadata.version"
```

### Task 6: `broker/features.rs` re-exports the shared table

**Files:**
- Modify: `crates/broker/src/features.rs` (re-export from `crabka_metadata`; keep `SupportedFeature` + `lookup` + `METADATA_VERSION_MAX`)
- Test: inline `#[cfg(test)]` in `features.rs` (existing tests must still pass)

- [ ] **Step 1: Update the failing test expectations**

Replace the existing `metadata_version_is_supported` test body in `crates/broker/src/features.rs` with:
```rust
    #[test]
    fn metadata_version_is_supported() {
        let f = lookup(METADATA_VERSION).expect("metadata.version supported");
        assert_eq!(f.min_version, crabka_metadata::metadata_version::METADATA_VERSION_MIN);
        assert_eq!(f.max_version, crabka_metadata::metadata_version::METADATA_VERSION_MAX);
        assert_eq!(f.min_version, 7);
        assert_eq!(f.max_version, 25);
        assert!(lookup("not.a.feature").is_none());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker features::`
Expected: FAIL — `min_version`/`max_version` still read the old `1`/`1`.

- [ ] **Step 3: Re-point the constants at the shared table**

Edit `crates/broker/src/features.rs` so `METADATA_VERSION`, `METADATA_VERSION_MIN`, `METADATA_VERSION_MAX` re-export the `crabka_metadata` values:
```rust
/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
/// Minimum supported `metadata.version` level.
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MIN;
/// Maximum supported `metadata.version` level.
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MAX;
```
Delete the old `const METADATA_VERSION* ` definitions. Keep `SupportedFeature`, `supported_features()` (now reading `METADATA_VERSION_MIN`/`MAX`), and `lookup()` unchanged. Update the module doc comment to note the table now lives in `crabka_metadata`.

Also add the per-RPC gate helper here (Batch 4's Tasks 9 and 10 both consume it; landing it in Task 6 keeps them parallel and conflict-free):
```rust
/// True when a feature requiring `required_level` must be blocked given
/// the `finalized` metadata.version. A missing finalized level (`None`,
/// `MetadataVersion.UNKNOWN`) is permissive — there is no level to gate
/// against — matching the runtime range guard's treatment.
pub(crate) fn metadata_version_blocks(finalized: Option<i16>, required_level: i16) -> bool {
    finalized.is_some_and(|level| level < required_level)
}
```
Add a unit test for it in the inline `tests` module:
```rust
    #[test]
    fn metadata_version_blocks_is_permissive_on_unknown() {
        assert!(!metadata_version_blocks(None, 11));
        assert!(metadata_version_blocks(Some(10), 11));
        assert!(!metadata_version_blocks(Some(11), 11));
    }
```

- [ ] **Step 4: Verify the whole broker crate (incl. `api_versions`) still passes**

Run: `cargo test -p crabka-broker features:: api_versions::`
Expected: PASS. `api_versions.rs` reads `crate::features::METADATA_VERSION_MAX`, which now resolves to `25`; its `supported_features_advertise_metadata_version` test asserts against the same constant, so it tracks automatically.

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-broker && cargo clippy -p crabka-broker -- -D warnings`
```bash
git add crates/broker/src/features.rs
git commit -m "refactor(broker): features.rs re-exports shared metadata.version table"
```

### Task 7: `UpdateFeatures` downgrade-safety floor

**Files:**
- Modify: `crates/broker/src/handlers/update_features.rs` (floor check + test)
- Test: `crates/broker/tests/update_features.rs` (integration) and/or inline unit test

- [ ] **Step 1: Write the failing test**

Add to the inline `tests` module in `crates/broker/src/handlers/update_features.rs` a pure-helper test:
```rust
    #[test]
    fn below_floor_is_rejected_even_with_downgrade_flag() {
        // floor = 14 (delegation tokens present); a finalize to 11 is an
        // unsafe downgrade and must be rejected regardless of the flag.
        assert!(violates_downgrade_floor(11, 14));
        assert!(!violates_downgrade_floor(14, 14));
        assert!(!violates_downgrade_floor(19, 14));
        // level 0 (delete) is handled separately; never a floor violation here.
        assert!(!violates_downgrade_floor(0, 14));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker update_features::tests::below_floor`
Expected: FAIL — `violates_downgrade_floor` not found.

- [ ] **Step 3: Implement the floor helper + wire it in**

Add the helper near `downgrade_allowed` in `crates/broker/src/handlers/update_features.rs`:
```rust
/// True if finalizing `metadata.version` to `level` would drop below the
/// `floor` the live image requires (KIP-584 unsafe downgrade). `level == 0`
/// (delete) is excluded — deletion is handled by the existing tombstone
/// path, not the floor.
fn violates_downgrade_floor(level: i16, floor: i16) -> bool {
    level > 0 && level < floor
}
```

In `handle`, inside the per-update loop, after the existing `level < 0 || level > feat.max_version` range check and before the `level == 0` block, add a floor check **scoped to `metadata.version`**:
```rust
        if name == crate::features::METADATA_VERSION {
            let floor = image.min_required_metadata_version();
            if violates_downgrade_floor(level, floor) {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not downgrade metadata.version below the level required by existing cluster state.",
                ));
                continue;
            }
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-broker update_features`
Expected: PASS.

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-broker && cargo clippy -p crabka-broker -- -D warnings`
```bash
git add crates/broker/src/handlers/update_features.rs
git commit -m "feat(broker): UpdateFeatures refuses unsafe metadata.version downgrades"
```

### Task 8: operator init-container `--release-version` handoff

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs` (`INIT_SCRIPT`, `render_init_container` signature + env, callers, tests)
- Test: inline `#[cfg(test)]` in `kafka_node_pool.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/operator/src/controller/kafka_node_pool.rs`:
```rust
#[test]
fn init_script_passes_release_version() {
    assert!(
        INIT_SCRIPT.contains("--release-version \"$CRABKA_METADATA_VERSION\""),
        "init script must pass the resolved metadata.version to crabka format"
    );
}

#[test]
fn init_container_wires_metadata_version_env() {
    let c = render_init_container("img:tag", "sec", 0, "4.0");
    let env = c["env"].as_array().expect("env array");
    let mv = env
        .iter()
        .find(|e| e["name"] == "CRABKA_METADATA_VERSION")
        .expect("CRABKA_METADATA_VERSION env present");
    assert_eq!(mv["value"], "4.0");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-operator kafka_node_pool::tests::init_`
Expected: FAIL — `INIT_SCRIPT` lacks the flag; `render_init_container` arity mismatch.

- [ ] **Step 3: Implement the script + env wiring**

Edit `INIT_SCRIPT` line 184 so the format invocation reads:
```
  /usr/bin/crabka format --log-dir /var/lib/crabka/data --cluster-id \"$CRABKA_CLUSTER_ID\" --release-version \"$CRABKA_METADATA_VERSION\"\n\
```

Change `render_init_container` to take the resolved metadata version and emit the env var:
```rust
fn render_init_container(
    broker_image: &str,
    secret_name: &str,
    node_id_start: i32,
    metadata_version: &str,
) -> serde_json::Value {
    json!({
        "name": "format",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [INIT_SCRIPT],
        "env": [
            { "name": "NODE_ID_START", "value": node_id_start.to_string() },
            { "name": "CRABKA_CLUSTER_ID", "valueFrom": { "secretKeyRef": { "name": secret_name, "key": "clusterId" } } },
            { "name": "CRABKA_METADATA_VERSION", "value": metadata_version.to_string() }
        ],
        "volumeMounts": [{ "name": "data", "mountPath": "/var/lib/crabka/data" }],
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "capabilities": { "drop": ["ALL"] }
        }
    })
}
```

Find the single caller of `render_init_container` in this file and thread the resolved metadata version (the same `resolved_metadata` the reconcile path already computes from `version::evaluate` / status) into the new argument. If the caller does not yet have the resolved value in scope, pass it down from the StatefulSet-render entry point's existing `metadata_version` parameter (the same value rendered into `[server_properties]` by `common.rs`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-operator kafka_node_pool`
Expected: PASS. (Existing init-script regression tests at lines ~1441–1455 still assert `crabka format` is present and `.node-id` is written after it — both still hold.)

- [ ] **Step 5: fmt + clippy + commit**

Run: `cargo fmt -p crabka-operator && cargo clippy -p crabka-operator -- -D warnings`
```bash
git add crates/operator/src/controller/kafka_node_pool.rs
git commit -m "feat(operator): pass resolved metadata.version to format init container"
```

---

## Batch 4

### Task 9: `AlterUserScramCredentials` per-RPC gate

**Files:**
- Modify: `crates/broker/src/handlers/alter_user_scram_credentials.rs` (gate at handler entry + test)
- Test: inline `#[cfg(test)]` in the handler

- [ ] **Step 1: Read the handler entry**

Run: `sed -n '1,60p' crates/broker/src/handlers/alter_user_scram_credentials.rs`
Identify the `handle(...)` signature and how it obtains the image (it takes `broker` and calls `broker.controller.current_image()` like `update_features`).

- [ ] **Step 2: Write the failing test**

Add a pure-helper test to the handler's inline `tests` module:
```rust
    #[test]
    fn scram_gate_permits_unknown_and_at_or_above_level() {
        // None (UNKNOWN) is permissive; below SCRAM level is gated.
        assert!(!metadata_version_blocks(None, crabka_metadata::metadata_version::SCRAM_MIN_LEVEL));
        assert!(metadata_version_blocks(Some(10), crabka_metadata::metadata_version::SCRAM_MIN_LEVEL));
        assert!(!metadata_version_blocks(Some(11), crabka_metadata::metadata_version::SCRAM_MIN_LEVEL));
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p crabka-broker alter_user_scram_credentials::tests::scram_gate`
Expected: FAIL — `metadata_version_blocks` not found.

- [ ] **Step 4: Wire the gate into the handler**

The `crate::features::metadata_version_blocks` helper was landed in Task 6 (Batch 3); this task only consumes it. The Step 2 test references it via `crate::features::metadata_version_blocks`.

In `alter_user_scram_credentials::handle`, after the image is fetched and the authorization gate passes, before processing any credential mutations, add:
```rust
    if crate::features::metadata_version_blocks(
        image.finalized_metadata_version(),
        crabka_metadata::metadata_version::SCRAM_MIN_LEVEL,
    ) {
        // metadata.version too low for KRaft SCRAM (3.5-IV2).
        return top_level_scram_error(
            codes::UNSUPPORTED_VERSION,
            "SCRAM is not enabled at the cluster's metadata.version.",
        );
    }
```
Use the handler's existing top-level error constructor (mirror how it returns an authorization failure). If none exists, build the error response inline matching the handler's response type. Confirm `codes::UNSUPPORTED_VERSION` exists (`= 35`); add it to `crates/broker/src/codes.rs` if absent.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-broker alter_user_scram_credentials`
Expected: PASS.

- [ ] **Step 6: fmt + clippy + commit**

Run: `cargo fmt -p crabka-broker && cargo clippy -p crabka-broker -- -D warnings`
```bash
git add crates/broker/src/handlers/alter_user_scram_credentials.rs crates/broker/src/codes.rs
git commit -m "feat(broker): gate AlterUserScramCredentials on metadata.version"
```

### Task 10: delegation-token per-RPC gates

**Files:**
- Modify: `crates/broker/src/handlers/create_delegation_token.rs`
- Modify: `crates/broker/src/handlers/renew_delegation_token.rs`
- Modify: `crates/broker/src/handlers/expire_delegation_token.rs`
- Test: inline `#[cfg(test)]` in `create_delegation_token.rs`

> Depends on Task 6 (the `metadata_version_blocks` helper) and Task 2. Does not touch `features.rs` or any file Task 9 touches — fully parallel with Task 9.

- [ ] **Step 1: Read each handler entry**

Run: `sed -n '1,60p' crates/broker/src/handlers/create_delegation_token.rs`
Note the `handle` signature, how it fetches the image, and its existing early-return error path (these handlers already return `DELEGATION_TOKEN_AUTH_DISABLED` (61) when no master key is configured — mirror that return shape).

- [ ] **Step 2: Write the failing test**

Add to `create_delegation_token.rs` inline `tests`:
```rust
    #[test]
    fn token_gate_uses_delegation_token_level() {
        use crabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL;
        assert!(!crate::features::metadata_version_blocks(None, DELEGATION_TOKEN_MIN_LEVEL));
        assert!(crate::features::metadata_version_blocks(Some(13), DELEGATION_TOKEN_MIN_LEVEL));
        assert!(!crate::features::metadata_version_blocks(Some(14), DELEGATION_TOKEN_MIN_LEVEL));
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p crabka-broker create_delegation_token::tests::token_gate`
Expected: FAIL (compile error until Step 4 imports resolve, or assertion if helper missing — confirm Task 6 landed `metadata_version_blocks`).

- [ ] **Step 4: Wire the gate into all three handlers**

In each of `create_delegation_token::handle`, `renew_delegation_token::handle`, `expire_delegation_token::handle`, after the image is fetched and before the existing master-key / auth checks return their token-specific response, add:
```rust
    if crate::features::metadata_version_blocks(
        image.finalized_metadata_version(),
        crabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    ) {
        // metadata.version too low for KRaft delegation tokens (3.6-IV2).
        return <existing error constructor>(
            codes::UNSUPPORTED_VERSION,
            "Delegation tokens are not enabled at the cluster's metadata.version.",
        );
    }
```
Replace `<existing error constructor>` with whatever each handler uses to build a top-level error response (the same shape as its `DELEGATION_TOKEN_AUTH_DISABLED` return). Each handler builds its own response type; do not share a constructor across the three if their response types differ.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-broker delegation_token`
Expected: PASS.

- [ ] **Step 6: fmt + clippy + commit**

Run: `cargo fmt -p crabka-broker && cargo clippy -p crabka-broker -- -D warnings`
```bash
git add crates/broker/src/handlers/create_delegation_token.rs \
        crates/broker/src/handlers/renew_delegation_token.rs \
        crates/broker/src/handlers/expire_delegation_token.rs
git commit -m "feat(broker): gate delegation-token RPCs on metadata.version"
```

---

## Batch 5

### Task 11: integration coverage, STATUS.md, jvm_acceptance

**Files:**
- Create/Modify: `crates/broker/tests/update_features.rs` (floor integration test)
- Modify: `crates/broker/tests/api_versions_features.rs` (advertised range)
- Modify: `STATUS.md` (new slice entry; flip the Slice 28 deferral)
- Modify: jvm_acceptance harness (re-run + assert advertised range / format flag)

- [ ] **Step 1: Broker integration test — bootstrap + floor**

Add to `crates/broker/tests/update_features.rs` an end-to-end test that formats with `--release-version 4.0`, brings up a broker, and asserts:
```rust
// 1. ApiVersions reports finalized metadata.version = 25 and epoch >= 0.
// 2. UpdateFeatures to level 24 succeeds (downgrade with flag, above floor).
// 3. With a SCRAM credential present, UpdateFeatures to level 10 (< 11)
//    is rejected with INVALID_UPDATE_VERSION even with the downgrade flag.
```
Follow the existing harness pattern in that file (it already drives `UpdateFeatures` against a live broker). Use the existing test helpers for formatting/boot; pass the new `release_version`.

Run: `cargo test -p crabka-broker --test update_features`
Expected: PASS.

- [ ] **Step 2: ApiVersions advertised-range test**

In `crates/broker/tests/api_versions_features.rs`, update/extend the assertion so `supported_features` for `metadata.version` reports `min_version = 7`, `max_version = 25`.

Run: `cargo test -p crabka-broker --test api_versions_features`
Expected: PASS.

- [ ] **Step 3: Full workspace gate**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all PASS / clean. Fix any fallout (e.g. other tests asserting the old `METADATA_VERSION_MAX = 1`).

- [ ] **Step 4: jvm_acceptance re-run (REQUIRED for raising MAX)**

Per STATUS.md, raising `METADATA_VERSION_MAX` requires re-running the Docker `jvm_acceptance` suite. Run it, and add/confirm probes that:
- `kafka-features describe` against the broker shows `metadata.version` supported max = 25.
- `kafka-storage format --release-version 4.0` parity: a JVM admin client negotiates the advertised range against cp-kafka 4.0 without `fromFeatureLevel` errors.

Run: the project's jvm_acceptance entry point (e.g. `cargo xtask jvm-acceptance` or the documented Docker command — check `tools/` / CI config for the exact invocation).
Expected: suite green.

- [ ] **Step 5: STATUS.md slice entry**

Add a new slice entry documenting the work, and update the Slice 28 note (lines ~1556–1560, ~1610–1611) to reflect that broker-side `metadata.version` enforcement is now implemented (no longer deferred). Summarize: table in `crabka_metadata` (MIN=7/MAX=25), bootstrap via `crabka format --release-version`, fail-fast range guard, downgrade floor, per-RPC gates, operator `MetadataVersionTooLow` + init-container handoff.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/tests/ STATUS.md <jvm_acceptance files>
git commit -m "test: metadata.version enforcement integration + jvm_acceptance + STATUS"
```

---

## Self-review

- **Spec coverage:**
  - §1 table → Task 1. §2 bootstrap → Task 3 (CLI) + Task 8 (operator handoff). §3 range guard → Task 5 (covers both startup-load-via-replay/install_snapshot and runtime post-commit through the single state-machine site). §4 downgrade floor → Task 2 (floor fn) + Task 7 (wiring). §5 per-RPC gates → Task 9 (SCRAM) + Task 10 (delegation tokens), helper in Task 6. §6 operator → Task 4 (`MetadataVersionTooLow`) + Task 8 (handoff). §7 testing/jvm_acceptance → Task 11. §8 out-of-scope → no tasks (correct).
- **Placeholder scan:** the only intentionally-open spots are (a) the exact `render_init_container` caller threading in Task 8 Step 3 and (b) each delegation-token handler's existing error constructor in Task 10 Step 4 — both are "match the existing pattern in this file" instructions with the surrounding code identified, not blanks. The level table is concrete and gated behind an explicit cp-kafka 4.0 verification step (Task 1 Step 1).
- **Type consistency:** `metadata_version_blocks(Option<i16>, i16) -> bool`, `finalized_metadata_version() -> Option<i16>`, `min_required_metadata_version() -> i16`, `from_version_string(&str) -> Option<MetadataVersion>`, `is_supported_level(i16) -> bool`, `resolve_release_level(&str) -> Result<i16, String>`, `violates_downgrade_floor(i16, i16) -> bool`, `metadata_version_out_of_range(Option<i16>) -> bool` — names and signatures are used identically across the tasks that reference them. The `metadata_version_blocks` helper is explicitly placed in Task 6 (not Task 9) so Batch 4 stays parallel.
- **Batch conflict check:** no two tasks in the same batch edit the same file. Batch 3's Tasks 6 and 7 share the `crates/broker` crate but distinct files and no new cross-references. Batch 4's Tasks 9 and 10 share `crates/broker` but distinct handler files, and the shared helper they both call was pre-landed in Batch 3 (Task 6).
