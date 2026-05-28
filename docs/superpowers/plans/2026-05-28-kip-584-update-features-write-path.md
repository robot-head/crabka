# KIP-584 UpdateFeatures Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `UpdateFeatures` RPC (api_key 57, KIP-584) so an operator can finalize the `metadata.version` feature through a Raft-persisted, ACL-gated write path, and surface the finalized features + a real epoch through `ApiVersions`.

**Architecture:** A new broker-side supported-features table drives both `ApiVersions` advertising and `UpdateFeatures` validation. `UpdateFeatures` is intercepted inline in `network::dispatch` (like `AlterUserScramCredentials`, api_key 51) so it can read the connection principal + peer for the `Cluster:Alter` ACL gate. Accepted updates become `V1FeatureLevel` metadata records committed via `controller.submit_change`; `MetadataImage` tracks a `feature_levels` map and a monotonic `features_epoch`. `ApiVersions` reads finalized features + epoch from the image; a fresh broker (no `UpdateFeatures` ever applied) keeps `finalized_features` empty and `finalized_features_epoch == -1`.

**Compatibility constraint (read this first):** A prior attempt to advertise `metadata.version` took down 19 `jvm_acceptance` tests because JVM clients call `MetadataVersion.fromFeatureLevel(N)` and throw on a level their enum doesn't enumerate. The newest validating client in our suite is Kafka 3.5 (`cp-kafka:7.5.0`). To stay safe we advertise `metadata.version` with a **single conservative level (1 = `3.0-IV1`, known to every KRaft-aware client ≥ 3.0)**, exposed as one tunable constant `METADATA_VERSION_MAX`. Task 9 verifies the JVM suite; if level 1 still breaks a client, lower the advertised surface there, do not scatter the constant.

**Tech Stack:** Rust, `crabka-protocol` (generated codecs), `crabka-metadata` (records + image), `openraft` controller, `tokio`. Tests: `cargo test`, plus Docker-gated `jvm_acceptance` integration tests.

---

## File Structure

- `crates/broker/src/codes.rs` — add `INVALID_UPDATE_VERSION` (95), `FEATURE_UPDATE_FAILED` (96).
- `crates/broker/src/features.rs` *(new)* — supported-feature table + `metadata.version` constants; shared by `api_versions` and `update_features`.
- `crates/metadata/src/records.rs` — add `FeatureLevelRecord` struct + `V1FeatureLevel` enum variant + round-trip test.
- `crates/metadata/src/lib.rs` — re-export `FeatureLevelRecord`.
- `crates/metadata/src/image.rs` — `feature_levels` map + `features_epoch` field; `apply`/`validate` arms; `finalized_features()` / `finalized_features_epoch()` accessors + tests.
- `crates/broker/src/handlers/update_features.rs` *(new)* — decode-free handler `handle(broker, req, version, ctx)`: ACL, per-feature validation, `validate_only`, Raft persistence, response assembly.
- `crates/broker/src/handlers/mod.rs` — `pub(crate) mod features;`? (no — `features` is a top-level broker module) + `pub(crate) mod update_features;` + a build_table comment noting api_key 57 is intercepted inline.
- `crates/broker/src/lib.rs` — `mod features;` (top-level).
- `crates/broker/src/network/dispatch.rs` — inline intercept for api_key 57, `handle_update_features_frame`, `handler_body_flexible` case 57.
- `crates/broker/src/handlers/api_versions.rs` — `supported_feature_keys()` / `finalized_feature_keys(image)` / epoch read from image + `features` table; update unit test.
- `crates/broker/tests/api_versions_features.rs` — update the regression test for the new (non-empty supported) surface; add a post-`UpdateFeatures` finalized-surface assertion is covered in the new integration test instead.
- `crates/broker/tests/update_features.rs` *(new)* — happy path, `validate_only`, ACL deny, unsupported feature, downgrade rejection, finalized-surface-after-set.

---

### Task 1: Error codes

**Files:**
- Modify: `crates/broker/src/codes.rs`

- [ ] **Step 1: Add the two KIP-584 error codes**

In `crates/broker/src/codes.rs`, after the existing `CLUSTER_AUTHORIZATION_FAILED` block (around line 126), add:

```rust
/// `INVALID_UPDATE_VERSION` (95, KIP-584) — a feature-level update in
/// `UpdateFeatures` is outside the broker's supported range, or attempts an
/// unguarded downgrade / deletion of a finalized feature.
pub const INVALID_UPDATE_VERSION: i16 = 95;

/// `FEATURE_UPDATE_FAILED` (96, KIP-584) — the cluster failed to persist a
/// validated feature update (e.g., the metadata write to Raft was rejected
/// or timed out).
pub const FEATURE_UPDATE_FAILED: i16 = 96;
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: builds (warnings about unused consts are fine until later tasks use them).

- [ ] **Step 3: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/codes.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(codes): add INVALID_UPDATE_VERSION + FEATURE_UPDATE_FAILED (KIP-584)"
```

---

### Task 2: `FeatureLevelRecord` metadata record

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/src/lib.rs`
- Test: `crates/metadata/src/records.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing round-trip test**

In the `#[cfg(test)] mod tests` block of `crates/metadata/src/records.rs`, add:

```rust
    #[test]
    fn feature_level_round_trip() {
        let r = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        });
        assert_eq!(round_trip(&r), r);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-metadata feature_level_round_trip`
Expected: FAIL — `FeatureLevelRecord` / `V1FeatureLevel` not found.

- [ ] **Step 3: Add the struct + enum variant**

In `crates/metadata/src/records.rs`, after the `DeleteDelegationTokenRecord` struct (around line 158), add:

```rust
/// KIP-584 finalized feature level. `level` is the finalized
/// `max_version_level` for `name`. `level == 0` is the KIP-584 sentinel
/// for "delete this finalized feature" — `MetadataImage::apply` removes the
/// entry rather than storing a zero. Replacement semantics: a later record
/// with the same `name` overwrites the previous level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureLevelRecord {
    pub name: String,
    pub level: i16,
}
```

Then add the variant to the `MetadataRecord` enum (after `V1UnregisterBroker`):

```rust
    V1FeatureLevel(FeatureLevelRecord),
```

- [ ] **Step 4: Re-export from the crate root**

In `crates/metadata/src/lib.rs`, add `FeatureLevelRecord` to the `pub use records::{...}` list (keep alphabetical-ish ordering — insert after `DeleteTopicRecord,`):

```rust
    DeleteTopicRecord, FeatureLevelRecord, MetadataRecord, NodeId, PartitionRecord, QuotaEntity,
    ScramCredentialRecord, TopicConfigRecord, TopicRecord, UnregisterBrokerRecord,
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-metadata feature_level_round_trip`
Expected: PASS.

Note: adding a `#[non_exhaustive]` enum variant will make `MetadataImage::apply` / `validate` non-exhaustive — those are fixed in Task 3, which must land before `crabka-metadata` builds clean. Run `cargo build -p crabka-metadata` now and expect a "non-exhaustive patterns" error in `image.rs`; that is resolved in Task 3.

- [ ] **Step 6: Commit (after Task 3 makes the crate build)**

Defer the commit to the end of Task 3 so the crate compiles. (No commit here.)

---

### Task 3: `MetadataImage` feature index + epoch

**Files:**
- Modify: `crates/metadata/src/image.rs`
- Test: `crates/metadata/src/image.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing tests**

In the `#[cfg(test)] mod tests` block of `crates/metadata/src/image.rs`, add (and add `FeatureLevelRecord` to the `use crate::records::{...}` import at the top of the test module):

```rust
    #[test]
    fn fresh_image_has_no_features_and_unknown_epoch() {
        let m = img();
        assert!(m.finalized_features().is_empty());
        assert_eq!(m.finalized_features_epoch(), -1);
    }

    #[test]
    fn apply_feature_level_sets_level_and_bumps_epoch() {
        let mut m = img();
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        }));
        assert_eq!(m.finalized_features().get("metadata.version"), Some(&1));
        assert_eq!(m.finalized_features_epoch(), 0);

        // A second apply bumps the epoch again (monotonic).
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        }));
        assert_eq!(m.finalized_features_epoch(), 1);
    }

    #[test]
    fn apply_feature_level_zero_deletes_entry() {
        let mut m = img();
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        }));
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 0,
        }));
        assert!(m.finalized_features().get("metadata.version").is_none());
        // Epoch still advanced — it is monotonic, not a count of live features.
        assert_eq!(m.finalized_features_epoch(), 1);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p crabka-metadata finalized_features`
Expected: FAIL — `finalized_features` / `finalized_features_epoch` not defined, and non-exhaustive match from Task 2.

- [ ] **Step 3: Add the fields**

In `crates/metadata/src/image.rs`, in `struct MetadataImage` (around line 56), add two fields after `delegation_tokens`:

```rust
    feature_levels: BTreeMap<String, i16>,
    /// KIP-584 finalized-features epoch. `-1` until the first
    /// `V1FeatureLevel` record applies, then monotonically increasing
    /// (one bump per applied record). Deterministic across replicas
    /// because records apply in committed-log order on every node.
    features_epoch: i64,
```

In `MetadataImage::new`, initialise them (after `delegation_tokens: HashMap::new(),`):

```rust
            feature_levels: BTreeMap::new(),
            features_epoch: -1,
```

(`Default` derive already covers `Default::default()` callers — but `new` is explicit, so set them explicitly. `BTreeMap` is already imported at the top via `use std::collections::{BTreeMap, HashMap};`.)

- [ ] **Step 4: Add accessors**

After the `delegation_token_by_hmac` accessor (around line 284), add:

```rust
    /// KIP-584: finalized feature levels, keyed by feature name. Empty
    /// until an `UpdateFeatures` call lands a `V1FeatureLevel` record.
    #[must_use]
    pub fn finalized_features(&self) -> &BTreeMap<String, i16> {
        &self.feature_levels
    }

    /// KIP-584 finalized-features epoch. `-1` ("unknown") until the first
    /// feature is finalized.
    #[must_use]
    pub fn finalized_features_epoch(&self) -> i64 {
        self.features_epoch
    }
```

- [ ] **Step 5: Add the `apply` arm**

In `MetadataImage::apply`, before the closing `}` of the match (after the `V1UnregisterBroker` arm, around line 399), add:

```rust
            MetadataRecord::V1FeatureLevel(rec) => {
                if rec.level == 0 {
                    self.feature_levels.remove(&rec.name);
                } else {
                    self.feature_levels.insert(rec.name.clone(), rec.level);
                }
                // Monotonic epoch: -1 -> 0 on the first record, then +1.
                self.features_epoch = self.features_epoch.saturating_add(1).max(0);
            }
```

- [ ] **Step 6: Add the `validate` arm**

In `MetadataImage::validate`, add `MetadataRecord::V1FeatureLevel(_)` to the unconditional-`Ok` arm list (alongside `V1UnregisterBroker(_)`):

```rust
            | MetadataRecord::V1UnregisterBroker(_)
            // KIP-584: feature-level admission is fully gated by the
            // UpdateFeatures handler (supported-range + downgrade checks);
            // image-level apply is an idempotent map upsert.
            | MetadataRecord::V1FeatureLevel(_) => Ok(()),
```

- [ ] **Step 7: Run tests to verify pass**

Run: `cargo test -p crabka-metadata`
Expected: PASS (all metadata tests, including Task 2's `feature_level_round_trip` and the three new image tests).

- [ ] **Step 8: Commit Tasks 2 + 3 together**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/metadata/src/records.rs crates/metadata/src/lib.rs crates/metadata/src/image.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "metadata: add FeatureLevelRecord + image feature index/epoch (KIP-584)"
```

---

### Task 4: Supported-features table

**Files:**
- Create: `crates/broker/src/features.rs`
- Modify: `crates/broker/src/lib.rs`
- Test: `crates/broker/src/features.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

Create `crates/broker/src/features.rs` with the test first:

```rust
//! KIP-584 supported-feature table. Drives both the `ApiVersions`
//! `supported_features` advertisement and the `UpdateFeatures` validation
//! path so the two can never disagree about what this broker supports.
//!
//! `metadata.version` is advertised at a single conservative level (1 =
//! `3.0-IV1`). JVM clients validate finalized + supported `metadata.version`
//! levels via `MetadataVersion.fromFeatureLevel(N)` and throw on a level
//! their enum doesn't know; level 1 is known to every KRaft-aware client
//! (Kafka >= 3.0). Raising `METADATA_VERSION_MAX` REQUIRES re-running the
//! Docker `jvm_acceptance` suite — see the slice plan's compatibility note.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub(crate) const METADATA_VERSION: &str = "metadata.version";
/// Minimum supported `metadata.version` level.
pub(crate) const METADATA_VERSION_MIN: i16 = 1;
/// Maximum supported `metadata.version` level. Conservative on purpose —
/// see the module note before raising it.
pub(crate) const METADATA_VERSION_MAX: i16 = 1;

/// One row of the supported-feature table.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features this broker supports finalizing.
pub(crate) fn supported_features() -> &'static [SupportedFeature] {
    const TABLE: &[SupportedFeature] = &[SupportedFeature {
        name: METADATA_VERSION,
        min_version: METADATA_VERSION_MIN,
        max_version: METADATA_VERSION_MAX,
    }];
    TABLE
}

/// Look up a supported feature by name.
pub(crate) fn lookup(name: &str) -> Option<SupportedFeature> {
    supported_features().iter().copied().find(|f| f.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_version_is_supported() {
        let f = lookup(METADATA_VERSION).expect("metadata.version supported");
        assert_eq!(f.min_version, 1);
        assert_eq!(f.max_version, METADATA_VERSION_MAX);
        assert!(lookup("not.a.feature").is_none());
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/broker/src/lib.rs`, add `mod features;` alongside the other top-level `mod` declarations (e.g. near `mod codes;`). If `codes` is `pub(crate) mod` or `mod`, match that visibility; `mod features;` (private to the crate) is sufficient since `handlers::*` and `network::*` are in the same crate.

- [ ] **Step 3: Run the test**

Run: `cargo test -p crabka-broker features::tests::metadata_version_is_supported`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/features.rs crates/broker/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(features): KIP-584 supported-feature table (metadata.version@1)"
```

---

### Task 5: `UpdateFeatures` handler

**Files:**
- Create: `crates/broker/src/handlers/update_features.rs`
- Modify: `crates/broker/src/handlers/mod.rs`
- Test: `crates/broker/src/handlers/update_features.rs` (inline `#[cfg(test)]`)

**Semantics (KIP-584), implemented in this task:**
- `Cluster:Alter` deny → top-level `CLUSTER_AUTHORIZATION_FAILED` (31), empty `results`.
- empty `feature_updates` → top-level `INVALID_REQUEST` (42), empty `results`.
- duplicate feature name in one request → that row gets `INVALID_REQUEST`.
- unknown / unsupported feature → row `INVALID_REQUEST`.
- `level > supported.max` or `level < 0` → row `INVALID_UPDATE_VERSION` (95).
- downgrade (`0 <= level < current_finalized`) without downgrade flag → row `INVALID_UPDATE_VERSION`. Downgrade flag = `allow_downgrade` on v0, or `upgrade_type ∈ {2,3}` on v1+.
- delete (`level == 0`) of a non-finalized feature → row `INVALID_UPDATE_VERSION`.
- `validate_only` → run all checks, never call `submit_change`.
- Raft submit failure: `RaftError::NotLeader` → request-wide `NOT_CONTROLLER` (41); any other → request-wide `FEATURE_UPDATE_FAILED` (96), applied to every row that had been `ok`.
- Top-level `error_code`: a request-wide failure code if present; otherwise, on v2 (no `results` on the wire) the first non-zero row code; otherwise `NONE`.

- [ ] **Step 1: Write the handler with failing unit tests**

Create `crates/broker/src/handlers/update_features.rs`:

```rust
//! `UpdateFeatures` handler (api_key 57, KIP-584).
//!
//! Finalizes broker-supported features (currently only `metadata.version`)
//! through a Raft-persisted `V1FeatureLevel` record. Gated by `Alter` on
//! `Cluster("kafka-cluster")`. Intercepted inline in `network::dispatch`
//! (like `AlterUserScramCredentials`) so the handler receives the
//! authenticated principal + peer for the ACL check.

use crabka_metadata::{AclOperation, FeatureLevelRecord, MetadataRecord};
use crabka_protocol::owned::update_features_request::UpdateFeaturesRequest;
use crabka_protocol::owned::update_features_response::{
    UpdatableFeatureResult, UpdateFeaturesResponse,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::RaftError;
use crate::features;

/// KIP-584 `FeatureUpdate.UpgradeType`: 1 = UPGRADE, 2 = SAFE_DOWNGRADE,
/// 3 = UNSAFE_DOWNGRADE.
fn downgrade_allowed(version: i16, allow_downgrade: bool, upgrade_type: i8) -> bool {
    if version == 0 {
        allow_downgrade
    } else {
        matches!(upgrade_type, 2 | 3)
    }
}

pub(crate) async fn handle(
    broker: &Broker,
    req: UpdateFeaturesRequest,
    version: i16,
    ctx: &crate::handlers::RequestContext<'_>,
) -> UpdateFeaturesResponse {
    let image = broker.controller.current_image();

    // Whole-request Cluster:Alter gate.
    let authorized = broker.config.authorizer.authorize(
        &image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: AclOperation::Alter,
        },
    ) == AuthorizationResult::Allow;

    if !authorized {
        return top_level_error(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
            version,
        );
    }

    if req.feature_updates.is_empty() {
        return top_level_error(
            codes::INVALID_REQUEST,
            "Can not provide empty feature updates in the request.",
            version,
        );
    }

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut results: Vec<UpdatableFeatureResult> = Vec::new();
    let mut records: Vec<MetadataRecord> = Vec::new();

    for upd in &req.feature_updates {
        let name = upd.feature.clone();
        if !seen.insert(name.clone()) {
            results.push(row(
                name,
                codes::INVALID_REQUEST,
                "Provided feature can not be updated more than once in the request.",
            ));
            continue;
        }
        let Some(feat) = features::lookup(&name) else {
            results.push(row(
                name,
                codes::INVALID_REQUEST,
                "Could not apply finalized feature update because the provided feature is not supported.",
            ));
            continue;
        };

        let level = upd.max_version_level;
        let current = image.finalized_features().get(&name).copied();
        let allow_dg = downgrade_allowed(version, upd.allow_downgrade, upd.upgrade_type);

        if level < 0 || level > feat.max_version {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Provided version level is not in the supported range.",
            ));
            continue;
        }
        if level == 0 {
            // Delete the finalized feature; only valid if it exists and a
            // downgrade is permitted.
            if current.is_none() {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not delete a finalized feature that does not exist.",
                ));
                continue;
            }
            if !allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not delete a finalized feature without setting the downgrade flag.",
                ));
                continue;
            }
        } else if let Some(cur) = current
            && level < cur
            && !allow_dg
        {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not downgrade a finalized feature without setting the downgrade flag.",
            ));
            continue;
        }

        // Accepted.
        records.push(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: name.clone(),
            level,
        }));
        results.push(row(name, codes::NONE, ""));
    }

    // validate_only: never persist.
    if req.validate_only {
        return finalize(results, version);
    }

    if !records.is_empty() {
        match broker.controller.submit_change(records).await {
            Ok(()) => {}
            Err(RaftError::NotLeader { .. }) => {
                return apply_request_wide(
                    results,
                    codes::NOT_CONTROLLER,
                    "This broker is not the active controller.",
                    version,
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "UpdateFeatures: submit_change failed");
                return apply_request_wide(
                    results,
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to persist the feature update.",
                    version,
                );
            }
        }
    }

    finalize(results, version)
}

fn row(feature: String, error_code: i16, msg: &str) -> UpdatableFeatureResult {
    UpdatableFeatureResult {
        feature,
        error_code,
        error_message: (error_code != codes::NONE).then(|| msg.to_string()),
        ..Default::default()
    }
}

fn top_level_error(code: i16, msg: &str, version: i16) -> UpdateFeaturesResponse {
    let _ = version;
    UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: code,
        error_message: Some(msg.to_string()),
        results: Vec::new(),
        ..Default::default()
    }
}

/// Overwrite every `ok` row with a request-wide failure code, and set the
/// top-level error too.
fn apply_request_wide(
    mut results: Vec<UpdatableFeatureResult>,
    code: i16,
    msg: &str,
    version: i16,
) -> UpdateFeaturesResponse {
    for r in results.iter_mut().filter(|r| r.error_code == codes::NONE) {
        r.error_code = code;
        r.error_message = Some(msg.to_string());
    }
    let mut resp = finalize(results, version);
    resp.error_code = code;
    resp.error_message = Some(msg.to_string());
    resp
}

/// Assemble the final response. On v2 (no `results` array on the wire) the
/// top-level `error_code` must carry the first non-zero row code so the
/// client still sees the failure.
fn finalize(results: Vec<UpdatableFeatureResult>, version: i16) -> UpdateFeaturesResponse {
    let (top_code, top_msg) = if version >= 2 {
        results
            .iter()
            .find(|r| r.error_code != codes::NONE)
            .map(|r| (r.error_code, r.error_message.clone()))
            .unwrap_or((codes::NONE, None))
    } else {
        (codes::NONE, None)
    };
    UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: top_code,
        error_message: top_msg,
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downgrade_flag_v0_uses_allow_downgrade() {
        assert!(downgrade_allowed(0, true, 1));
        assert!(!downgrade_allowed(0, false, 2));
    }

    #[test]
    fn downgrade_flag_v1_uses_upgrade_type() {
        assert!(!downgrade_allowed(1, true, 1)); // UPGRADE
        assert!(downgrade_allowed(1, false, 2)); // SAFE_DOWNGRADE
        assert!(downgrade_allowed(1, false, 3)); // UNSAFE_DOWNGRADE
    }

    #[test]
    fn row_sets_message_only_on_error() {
        assert!(row("metadata.version".into(), codes::NONE, "x").error_message.is_none());
        assert_eq!(
            row("metadata.version".into(), codes::INVALID_UPDATE_VERSION, "bad").error_message.as_deref(),
            Some("bad"),
        );
    }

    #[test]
    fn finalize_v2_promotes_first_error_to_top_level() {
        let results = vec![
            row("a".into(), codes::NONE, ""),
            row("b".into(), codes::INVALID_UPDATE_VERSION, "bad"),
        ];
        let resp = finalize(results, 2);
        assert_eq!(resp.error_code, codes::INVALID_UPDATE_VERSION);
    }

    #[test]
    fn finalize_v1_keeps_top_level_none() {
        let results = vec![row("b".into(), codes::INVALID_UPDATE_VERSION, "bad")];
        let resp = finalize(results, 1);
        assert_eq!(resp.error_code, codes::NONE);
    }
}
```

> **Implementer note:** confirm the exact path of `RaftError` (`crate::error::RaftError` re-exported, or `crabka_raft::RaftError`). Grep an existing handler that matches on `submit_change` errors — e.g. `rg "RaftError::NotLeader" crates/broker/src` — and use the same import path. Also confirm `broker.config.authorizer` and `broker.controller` field paths against `alter_user_scram_credentials.rs` (they are used identically there).

- [ ] **Step 2: Register the module**

In `crates/broker/src/handlers/mod.rs`, add the module declaration alphabetically among the other `pub(crate) mod` lines:

```rust
pub(crate) mod update_features;
```

And in `build_table()`, add a comment near the other inline-intercept notes (do NOT register a handler — it is intercepted inline):

```rust
    // UpdateFeatures (api_key 57, KIP-584) is intercepted inline in
    // `network::dispatch` so the handler can receive the per-connection
    // principal + peer `SocketAddr` for the Cluster:Alter ACL gate.
```

- [ ] **Step 3: Run the unit tests**

Run: `cargo test -p crabka-broker update_features::tests`
Expected: PASS (5 tests).

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/handlers/update_features.rs crates/broker/src/handlers/mod.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(handlers): UpdateFeatures handler with KIP-584 validation"
```

---

### Task 6: Wire `UpdateFeatures` into dispatch

**Files:**
- Modify: `crates/broker/src/network/dispatch.rs`

- [ ] **Step 1: Add the inline intercept**

In `crates/broker/src/network/dispatch.rs`, immediately after the api_key 51 intercept block (ends around line 399), add a parallel block for api_key 57:

```rust
        if peek_api_key(&frame).ok() == Some(57) {
            match handle_update_features_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during UpdateFeatures, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "UpdateFeatures dispatch error, closing connection");
                    break;
                }
            }
        }
```

- [ ] **Step 2: Add the frame handler**

Immediately after `handle_alter_user_scram_credentials_frame` (ends around line 1690), add:

```rust
async fn handle_update_features_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 57);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp = crate::handlers::update_features::handle(broker, req, api_version, &ctx).await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}
```

- [ ] **Step 3: Add the flexible-body case**

In the `handler_body_flexible` match (around line 3671-3730), add the case for api_key 57 (place near the other admin keys, before the `_ => false` arm):

```rust
        57 => version >= owned::update_features_request::FLEXIBLE_MIN,
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p crabka-broker`
Expected: builds clean.

- [ ] **Step 5: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/network/dispatch.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(dispatch): intercept UpdateFeatures (api_key 57) inline for ACL"
```

---

### Task 7: Surface features through `ApiVersions`

**Files:**
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Test: `crates/broker/src/handlers/api_versions.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Replace the feature stubs to read the table + image**

In `crates/broker/src/handlers/api_versions.rs`:

1. Remove the `const FINALIZED_FEATURES_EPOCH: i64 = -1;` (the epoch now comes from the image).
2. Replace `supported_feature_keys()` and `finalized_feature_keys()`:

```rust
fn supported_feature_keys() -> Vec<SupportedFeatureKey> {
    crate::features::supported_features()
        .iter()
        .map(|f| SupportedFeatureKey {
            name: f.name.to_string(),
            min_version: f.min_version,
            max_version: f.max_version,
            ..Default::default()
        })
        .collect()
}

fn finalized_feature_keys(image: &crabka_metadata::MetadataImage) -> Vec<FinalizedFeatureKey> {
    image
        .finalized_features()
        .iter()
        .map(|(name, level)| FinalizedFeatureKey {
            name: name.clone(),
            // Kafka reports the finalized level as both the min and max
            // finalized version level.
            max_version_level: *level,
            min_version_level: *level,
            ..Default::default()
        })
        .collect()
}
```

3. In `handle`, capture the image before the async move (alongside the existing `let metrics = broker.metrics.clone();`):

```rust
        let image = broker.controller.current_image();
```

4. In the accepted-response construction, replace the feature fields:

```rust
            supported_features: supported_feature_keys(),
            finalized_features_epoch: image.finalized_features_epoch(),
            finalized_features: finalized_feature_keys(&image),
```

(Update the module-level doc comment and the `FINALIZED_FEATURES_EPOCH` rustdoc block to describe the new behavior: supported `metadata.version` advertised from `crate::features`; finalized features + epoch read from the metadata image; `-1`/empty on a fresh broker.)

- [ ] **Step 2: Update the unit test**

Replace the `feature_surface_is_empty_with_unknown_epoch` test with:

```rust
    #[test]
    fn supported_features_advertise_metadata_version() {
        let keys = supported_feature_keys();
        let mv = keys
            .iter()
            .find(|k| k.name == "metadata.version")
            .expect("metadata.version advertised");
        assert_eq!(mv.min_version, 1);
        assert_eq!(mv.max_version, crate::features::METADATA_VERSION_MAX);
    }

    #[test]
    fn fresh_image_surfaces_no_finalized_features() {
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(finalized_feature_keys(&image).is_empty());
        assert_eq!(image.finalized_features_epoch(), -1);
    }
```

(Ensure `uuid` is available to the test module — it is a workspace dep; add `use` if needed, or fully-qualify as written.)

- [ ] **Step 3: Run the unit tests**

Run: `cargo test -p crabka-broker api_versions`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/handlers/api_versions.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(api_versions): surface KIP-584 supported + finalized features"
```

---

### Task 8: Integration tests

**Files:**
- Modify: `crates/broker/tests/api_versions_features.rs`
- Create: `crates/broker/tests/update_features.rs`

- [ ] **Step 1: Update the read-side regression test**

In `crates/broker/tests/api_versions_features.rs`, the existing test must change: `supported_features` is now NON-empty (advertises `metadata.version`), while `finalized_features` stays empty + epoch `-1` on a fresh broker. Replace the assertions in `v3_response_feature_surface_is_empty_with_unknown_epoch` (rename to `v3_response_advertises_supported_metadata_version_no_finalized`):

```rust
    assert_eq!(resp.error_code, 0, "{resp:?}");

    // KIP-584 write-side: supported_features now advertises
    // metadata.version (conservative level), but a fresh broker has no
    // finalized features and the epoch is the schema sentinel -1.
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version advertised in supported_features");
    assert_eq!(mv.min_version, 1, "{resp:?}");
    assert_eq!(mv.max_version, 1, "{resp:?}");
    assert!(
        resp.finalized_features.is_empty(),
        "fresh broker has no finalized features: {:?}",
        resp.finalized_features,
    );
    assert_eq!(
        resp.finalized_features_epoch, -1,
        "fresh broker epoch must be -1 until UpdateFeatures lands a record",
    );
```

Update the module doc comment to note the surface is now the write-side (supported advertised, finalized gated behind UpdateFeatures).

- [ ] **Step 2: Run it**

Run: `cargo test -p crabka-broker --test api_versions_features`
Expected: PASS.

- [ ] **Step 3: Write the UpdateFeatures integration test**

Create `crates/broker/tests/update_features.rs`. Use the same `support` harness the other broker integration tests use (mirror the top of `api_versions_features.rs`: `mod support;` then `support::start().await`). Inspect `crates/broker/tests/support/` for the client API (the harness exposes `p.client.send(req)` returning the typed response, and `p.broker.shutdown()`). The principal in the default harness is a super-user / ANONYMOUS-allowed path — confirm by checking how `alter_user_scram_credentials` integration tests (if any) authorize; if the default harness denies Cluster:Alter, configure `super_users` in the harness builder as those tests do.

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};

fn metadata_version_update(level: i16) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: vec![FeatureUpdateKey {
            feature: "metadata.version".into(),
            max_version_level: level,
            upgrade_type: 1,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn finalizes_metadata_version_and_surfaces_in_api_versions() {
    let p = support::start().await;

    let resp = p
        .client
        .send(metadata_version_update(1))
        .await
        .expect("UpdateFeatures");
    assert_eq!(resp.error_code, 0, "{resp:?}");
    let row = resp.results.iter().find(|r| r.feature == "metadata.version");
    if let Some(row) = row {
        assert_eq!(row.error_code, 0, "{resp:?}");
    }

    // ApiVersions now surfaces the finalized feature with a real epoch.
    let av = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    let fin = av
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized");
    assert_eq!(fin.max_version_level, 1, "{av:?}");
    assert!(av.finalized_features_epoch >= 0, "{av:?}");

    p.broker.shutdown().await;
}

#[tokio::test]
async fn rejects_unsupported_feature() {
    let p = support::start().await;
    let mut req = metadata_version_update(1);
    req.feature_updates[0].feature = "not.a.feature".into();
    let resp = p.client.send(req).await.expect("UpdateFeatures");
    let row = resp
        .results
        .iter()
        .find(|r| r.feature == "not.a.feature")
        .expect("row present");
    assert_eq!(row.error_code, 42, "INVALID_REQUEST for unsupported feature: {resp:?}");
    p.broker.shutdown().await;
}

#[tokio::test]
async fn rejects_level_above_supported_max() {
    let p = support::start().await;
    let resp = p
        .client
        .send(metadata_version_update(99))
        .await
        .expect("UpdateFeatures");
    let row = resp
        .results
        .iter()
        .find(|r| r.feature == "metadata.version")
        .expect("row present");
    assert_eq!(row.error_code, 95, "INVALID_UPDATE_VERSION: {resp:?}");
    p.broker.shutdown().await;
}

#[tokio::test]
async fn validate_only_does_not_persist() {
    let p = support::start().await;
    let mut req = metadata_version_update(1);
    req.validate_only = true;
    let resp = p.client.send(req).await.expect("UpdateFeatures");
    assert_eq!(resp.error_code, 0, "{resp:?}");

    // Nothing finalized.
    let av = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert!(
        av.finalized_features.is_empty(),
        "validate_only must not persist: {av:?}",
    );
    assert_eq!(av.finalized_features_epoch, -1, "{av:?}");
    p.broker.shutdown().await;
}
```

> **Implementer note:** the request must reach the broker as version ≥ 1 for `validate_only` to be encoded (v0 has no `validate_only` field). The `support` client sends at the codec's MAX_VERSION (2) by default — confirm, and if it pins a lower version, set it explicitly. If the default harness principal is denied `Cluster:Alter`, the happy-path tests will see `CLUSTER_AUTHORIZATION_FAILED` (31); configure the harness as a super-user (mirror whatever `support::start()` does for other admin RPCs — check `describe_acls`/`create_acls` integration tests).

- [ ] **Step 4: Run the integration tests**

Run: `cargo test -p crabka-broker --test update_features`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full broker test suite (no Docker)**

Run: `cargo test -p crabka-broker`
Expected: PASS. Pay attention to any other test that asserts on the `ApiVersions` feature surface.

- [ ] **Step 6: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/tests/api_versions_features.rs crates/broker/tests/update_features.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(test): UpdateFeatures write-path + finalized-surface integration tests"
```

---

### Task 9: JVM acceptance verification (the compatibility gate)

**Files:** none (verification only). Requires Docker.

- [ ] **Step 1: Run the workspace checks**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS (no Docker tests run; they are `#[ignore]`).

- [ ] **Step 2: Run the Docker JVM acceptance suite**

Run: `cargo test -p crabka-broker --test jvm_acceptance -- --ignored --test-threads=1`
Expected: PASS — every previously-passing JVM CLI test (kafka-acls, kafka-configs, kafka-topics, etc.) still passes with `metadata.version` now advertised at level 1. This is the regression that took down 19 tests before; it MUST be green.

- [ ] **Step 3: If any JVM test regresses**

The advertised `metadata.version` surface is the suspect. Triage in this order, smallest blast radius first:
1. Confirm which client/tool fails and its Kafka version (the test name + `KAFKA_IMAGE*` constant identify it).
2. If a client throws on the *supported* level, the safe surface is below level 1 — meaning the client cannot tolerate ANY advertised `metadata.version`. In that case, gate advertising: only emit the `metadata.version` supported key, and finalized keys, when at least one feature is finalized (i.e., keep `supported_features` empty on a fresh broker too) — adjust `supported_feature_keys()` to take the image and return empty when `image.finalized_features().is_empty()`. Re-run the suite.
3. Record the empirical finding in the `crate::features` module doc and in `api_versions_features.rs`.

- [ ] **Step 4: Final commit (if Step 3 changed code)**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add -A
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(features): pin metadata.version advertising to JVM-verified surface"
```

---

## Self-Review Notes

- **Spec coverage:** decode (Task 6) → ACL gate (Task 5) → per-feature validation incl. downgrade/delete (Task 5) → `validate_only` (Task 5/8) → Raft persistence via `V1FeatureLevel` (Tasks 2/3/5) → epoch (Task 3) → `ApiVersions` surfacing (Task 7) → JVM compat verification (Task 9). All covered.
- **Type consistency:** `FeatureLevelRecord { name, level }`, `MetadataRecord::V1FeatureLevel`, `MetadataImage::finalized_features()/finalized_features_epoch()`, `features::{METADATA_VERSION, METADATA_VERSION_MIN, METADATA_VERSION_MAX, supported_features, lookup, SupportedFeature}`, handler signature `handle(broker, req, version, ctx)` — used consistently across Tasks 2–8.
- **Open implementer confirmations (flagged inline):** exact `RaftError` import path; whether `support::start()` authorizes `Cluster:Alter` by default (super-user config); default client request version for `UpdateFeatures`. These are codebase facts to grep at implementation time, not design gaps.
