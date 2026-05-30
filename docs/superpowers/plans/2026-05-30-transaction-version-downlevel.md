# transaction.version (KIP-890) Downlevel Behavior Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Register `transaction.version` (KIP-890) in the feature framework, finalize it per-release at bootstrap, and make the transaction coordinator's record format, producer-epoch handling, and partition-verification behavior genuinely conditional on the finalized level so every level (TV_0 classic, TV_1 flexible records, TV_2 epoch-bump + server-side verification) behaves exactly like Kafka.

**Architecture:** Builds on the generalized feature framework from `2026-05-30-feature-framework-and-group-version.md` (the `Feature` trait + registry + `require_feature`). A `TransactionVersionFeature` registry row drives advertisement, bootstrap defaults, and dependency checks. A single `resolve_txn_version(&image)` helper in the txn module reads the finalized level once per request; the coordinator branches on it. TV_2 epoch-bump and verify-only `AddPartitionsToTxn` are *new* behavior implemented here (the coordinator today is effectively TV_0/classic: binary `serde-wincode` `__transaction_state` records, unused `verify_only`, classic InitProducerId-reuse fencing).

**Tech Stack:** Rust, `crabka-metadata` (feature registry, image), `crabka-broker` (`txn/` coordinator + handlers), `crabka-protocol` (txn request/response codecs). Tests: `cargo test`, plus the full Docker `jvm_acceptance` sweep as the final compatibility gate.

**Prerequisite:** The framework plan above MUST be merged first (`feature_registry`, `feature(name)`, `is_supported_level`, `features::require_feature`, multi-feature bootstrap, multi-feature range guard).

**Spec:** `docs/superpowers/specs/2026-05-30-feature-versioning-framework-group-txn-design.md`.

**Empirical-pinning rule (CLAUDE.md):** the TV range, per-release default, MV dependency, and especially the TV_1 *flexible `__transaction_state` record wire format* are pinned against cp-kafka 4.0 in Task 0 before any code consumes them. `⟨pin⟩` marks a provisional value.

---

## Reality note (read first)

The earlier assumption that Crabka "runs TV_2 behavior unconditionally" is **wrong**. The coordinator map shows:
- `__transaction_state` records use a binary `serde-wincode` codec (`SerdeCompat<TxnEntry>`), **not** Kafka's flexible record format → this is TV_0-shaped, not TV_1.
- `AddPartitionsToTxnTransaction.verify_only` exists in the schema but is **unused** → no TV_2 server-side verification.
- The producer-epoch bump happens on **InitProducerId reuse** (classic fencing), **not** on every EndTxn completion → not the TV_2 epoch-bump rule.

So this plan **implements** TV_1 and TV_2 behavior and gates it; the existing behavior becomes the TV_0 path. Existing txn integration tests assume a 4.0-default cluster and must stay green at TV_2.

---

## File Structure

- `crates/metadata/src/transaction_version.rs` *(new)* — `transaction.version` name + level constants + MV-dependency thresholds.
- `crates/metadata/src/feature.rs` — add `TransactionVersionFeature` + register it.
- `crates/metadata/src/lib.rs` — `mod transaction_version;` + re-export.
- `crates/metadata/src/image.rs` — add a generic `finalized_feature(name) -> Option<i16>` accessor (ergonomic read for the coordinator).
- `crates/broker/src/txn/version.rs` *(new)* — `TxnVersion` enum (`Classic`/`Flexible`/`Verified` for TV 0/1/2) + `resolve_txn_version(&image) -> TxnVersion`.
- `crates/broker/src/txn/coordinator.rs` — `put`/`recover` select record codec by TV; helper to read the resolved TV.
- `crates/broker/src/txn/state.rs` — flexible-codec encode/decode for `TxnEntry` (TV_1 path) alongside the existing binary codec (TV_0).
- `crates/broker/src/txn/handlers/end_txn.rs` — TV_2 epoch bump on completion.
- `crates/broker/src/txn/handlers/add_partitions_to_txn.rs` — TV_2 `verify_only` path.
- `crates/broker/src/handlers/init_producer_id.rs` — epoch handling consistent with TV (bump rule centralized).
- `crates/broker/tests/transaction_version.rs` *(new)* — per-level behavior integration tests.
- `STATUS.md`, `README.md` — slice entry + flip KIP-584/KIP-890 rows after the full jvm sweep.

**Batching (per CLAUDE.md):**
- **Batch 1 (sequential foundation):** Task 0 → Task 1 (feature registration) → Task 2 (`finalized_feature` accessor + `TxnVersion` resolver). Establishes the level all later tasks read.
- **Batch 2 (parallel — disjoint files):** Task 3 (`state.rs` flexible codec), Task 4 (`end_txn.rs` epoch bump), Task 5 (`add_partitions_to_txn.rs` verify path). Each gated by `TxnVersion` from Task 2; different files.
- **Batch 3 (sequential):** Task 6 (`coordinator.rs` codec selection wiring — depends on Task 3) → Task 7 (integration tests) → Task 8 (full jvm sweep + docs).

---

### Task 0: Pin transaction.version levels + semantics (Docker / empirical)

**Files:** none (produces verified constants + a documented format spec the rest of the plan consumes).

- [ ] **Step 1: Pin the range, default, and dependency**

Using a cp-kafka 4.0 container (as in the framework plan's Task 0), confirm:
- `transaction.version` supported range — expected `0..=2` `⟨pin⟩`.
- default at `--release-version 4.0` — expected `2` `⟨pin⟩`.
- the `metadata.version` levels TV_1 and TV_2 depend on (KIP-1022) — `⟨pin⟩` each.

- [ ] **Step 2: Pin the TV_1 flexible `__transaction_state` record format**

This is the critical wire detail. Determine the exact on-disk record schema Kafka writes for transaction state at TV_1 (the flexible/tagged-field `TransactionLogValue` schema). Sources, in order of preference:
1. The cp-kafka 4.0 `TransactionLogValue.json` message schema (search the image / kafka source jar):
   ```bash
   docker run --rm confluentinc/cp-kafka:7.9.0 bash -lc \
     'find / -name "TransactionLogValue*" 2>/dev/null; \
      find / -name "*.jar" | xargs -I{} sh -c "unzip -l {} 2>/dev/null | grep -i TransactionLogValue" 2>/dev/null | head'
   ```
2. Produce a transaction against a real cp-kafka 4.0 broker formatted at 4.0, then dump `__transaction_state-N` with `kafka-dump-log` to observe the value bytes.

Record the field list, types, tagged-field layout, and the value-version header. This is what Task 3 implements. **Do not invent it** — if it cannot be pinned, stop and flag; the plan cannot proceed correctly without it.

- [ ] **Step 3: Pin the TV_2 behaviors**

Confirm against cp-kafka 4.0:
- EndTxn at TV_2 bumps the producer epoch on completion and returns the bumped `producer_id`/`producer_epoch` in the response (EndTxn v5). Note the exact response fields.
- `AddPartitionsToTxn` v4+ with `verify_only=true` is broker-internal verification: it checks the partition is already part of the ongoing txn for `(producer_id, producer_epoch)` and returns success/`TRANSACTION_ABORTABLE` without adding. Note the error code used (`TRANSACTION_ABORTABLE` / `INVALID_TXN_STATE`) `⟨pin⟩`.

No commit (verification only). Write the findings into the module docs created in Task 1/Task 3.

---

### Task 1: Register `transaction.version` in the framework

**Files:**
- Create: `crates/metadata/src/transaction_version.rs`
- Modify: `crates/metadata/src/feature.rs` (add `TransactionVersionFeature` + register)
- Modify: `crates/metadata/src/lib.rs`
- Test: `crates/metadata/src/feature.rs` (inline)

- [ ] **Step 1: Create `crates/metadata/src/transaction_version.rs`**

Use Task 0's verified values for every `⟨pin⟩`.

```rust
//! KIP-890 `transaction.version` feature-level constants. Plain integer
//! feature: 0 = classic (KIP-98) non-flexible txn-state records; 1 = flexible
//! (tagged) txn-state records; 2 = epoch-bump-on-completion + server-side
//! AddPartitionsToTxn verification. Verify the range and the metadata.version
//! dependency thresholds against cp-kafka 4.0 before editing.

pub const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";
pub const TRANSACTION_VERSION_MIN: i16 = 0; // ⟨pin⟩
pub const TRANSACTION_VERSION_MAX: i16 = 2; // ⟨pin⟩

/// metadata.version at/above which transaction.version=1 (flexible records) is
/// the per-release default + dependency.
pub const TV1_METADATA_LEVEL: i16 = 0; // ⟨pin⟩
/// metadata.version at/above which transaction.version=2 is the default + dep.
pub const TV2_METADATA_LEVEL: i16 = 0; // ⟨pin⟩
```

- [ ] **Step 2: Add `TransactionVersionFeature` to `feature.rs` and register**

```rust
/// `transaction.version` (KIP-890). Default rises with the bootstrap
/// metadata.version. Downgrade floor is the supported min: in-flight txn state
/// lives in the __transaction_state log, not the MetadataImage, so a
/// live-state-aware floor cannot be computed here (deferred — see spec).
pub struct TransactionVersionFeature;

impl Feature for TransactionVersionFeature {
    fn name(&self) -> &'static str {
        crate::transaction_version::TRANSACTION_VERSION_FEATURE
    }
    fn supported_range(&self) -> (i16, i16) {
        (
            crate::transaction_version::TRANSACTION_VERSION_MIN,
            crate::transaction_version::TRANSACTION_VERSION_MAX,
        )
    }
    fn default_level(&self, bootstrap_mv: i16) -> i16 {
        use crate::transaction_version::{TV1_METADATA_LEVEL, TV2_METADATA_LEVEL};
        if bootstrap_mv >= TV2_METADATA_LEVEL {
            2
        } else if bootstrap_mv >= TV1_METADATA_LEVEL {
            1
        } else {
            0
        }
    }
    fn dependencies(&self, level: i16) -> &'static [(&'static str, i16)] {
        use crate::transaction_version::{TV1_METADATA_LEVEL, TV2_METADATA_LEVEL};
        match level {
            2 => {
                const D: &[(&str, i16)] =
                    &[(crate::metadata_version::METADATA_VERSION_FEATURE, TV2_METADATA_LEVEL)];
                D
            }
            1 => {
                const D: &[(&str, i16)] =
                    &[(crate::metadata_version::METADATA_VERSION_FEATURE, TV1_METADATA_LEVEL)];
                D
            }
            _ => &[],
        }
    }
}
```

Update `feature_registry`:

```rust
    const REGISTRY: &[&dyn Feature] =
        &[&MetadataVersionFeature, &GroupVersionFeature, &TransactionVersionFeature];
```

- [ ] **Step 3: `lib.rs` module + re-export**

Add `pub mod transaction_version;`.

- [ ] **Step 4: Unit tests in `feature.rs`**

```rust
    #[test]
    fn transaction_version_registered() {
        let f = feature("transaction.version").expect("registered");
        assert!(f.supported_range() == (0, 2));
    }

    #[test]
    fn transaction_version_default_follows_release() {
        let f = feature("transaction.version").unwrap();
        assert!(f.default_level(25) == 2); // 4.0 → TV_2
    }

    #[test]
    fn transaction_version_two_depends_on_metadata_version() {
        let f = feature("transaction.version").unwrap();
        assert!(!f.dependencies(2).is_empty());
        assert!(f.dependencies(0).is_empty());
    }
```

- [ ] **Step 5: Run + commit**

Run: `cargo test -p crabka-metadata feature:: transaction_version` → PASS.

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/metadata/src/transaction_version.rs crates/metadata/src/feature.rs crates/metadata/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "metadata: register transaction.version (KIP-890) feature with per-release default + MV deps

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `finalized_feature` accessor + `TxnVersion` resolver

**Files:**
- Modify: `crates/metadata/src/image.rs` (+ inline test)
- Create: `crates/broker/src/txn/version.rs`
- Modify: `crates/broker/src/txn/mod.rs` (register the module)
- Test: `crates/broker/src/txn/version.rs` (inline)

- [ ] **Step 1: Add the generic image accessor**

In `crates/metadata/src/image.rs`, after `finalized_metadata_version` (line ~340), add:

```rust
    /// KIP-584: the finalized level for an arbitrary feature, or `None` if it
    /// has not been finalized. Generic counterpart to
    /// [`Self::finalized_metadata_version`].
    #[must_use]
    pub fn finalized_feature(&self, name: &str) -> Option<i16> {
        self.feature_levels.get(name).copied()
    }
```

Add a quick test in the image `#[cfg(test)]` module:

```rust
    #[test]
    fn finalized_feature_reads_arbitrary_name() {
        let mut m = MetadataImage::new(uuid::Uuid::nil());
        assert!(m.finalized_feature("transaction.version").is_none());
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "transaction.version".into(),
            level: 2,
        }));
        assert!(m.finalized_feature("transaction.version") == Some(2));
    }
```

- [ ] **Step 2: Create `crates/broker/src/txn/version.rs`**

```rust
//! KIP-890 transaction.version resolution. Reads the finalized
//! `transaction.version` from the live image and maps it to the behavior the
//! coordinator runs. Unfinalized (UNKNOWN) resolves to `Classic` — the safest
//! behavior for a pre-bootstrap / legacy image; a 4.0-formatted cluster
//! finalizes TV_2 at bootstrap, so the common path is `Verified`.

use crabka_metadata::MetadataImage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnVersion {
    /// TV_0: classic (KIP-98), non-flexible `__transaction_state` records.
    Classic,
    /// TV_1: flexible (tagged) `__transaction_state` records.
    Flexible,
    /// TV_2: epoch bump on completion + server-side AddPartitionsToTxn
    /// verification (also flexible records).
    Verified,
}

impl TxnVersion {
    /// Flexible `__transaction_state` record format applies at TV >= 1.
    pub(crate) fn flexible_records(self) -> bool {
        matches!(self, TxnVersion::Flexible | TxnVersion::Verified)
    }
    /// Epoch-bump-on-completion + verify-only AddPartitionsToTxn apply at TV_2.
    pub(crate) fn verified(self) -> bool {
        matches!(self, TxnVersion::Verified)
    }
}

pub(crate) fn resolve_txn_version(image: &MetadataImage) -> TxnVersion {
    match image.finalized_feature(crabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE) {
        Some(2) => TxnVersion::Verified,
        Some(1) => TxnVersion::Flexible,
        _ => TxnVersion::Classic,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{FeatureLevelRecord, MetadataRecord};

    fn image_with_tv(level: Option<i16>) -> MetadataImage {
        let mut m = MetadataImage::new(uuid::Uuid::nil());
        if let Some(l) = level {
            m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "transaction.version".into(),
                level: l,
            }));
        }
        m
    }

    #[test]
    fn resolves_levels() {
        assert!(resolve_txn_version(&image_with_tv(None)) == TxnVersion::Classic);
        assert!(resolve_txn_version(&image_with_tv(Some(0))) == TxnVersion::Classic);
        assert!(resolve_txn_version(&image_with_tv(Some(1))) == TxnVersion::Flexible);
        assert!(resolve_txn_version(&image_with_tv(Some(2))) == TxnVersion::Verified);
    }

    #[test]
    fn behavior_predicates() {
        assert!(!TxnVersion::Classic.flexible_records());
        assert!(TxnVersion::Flexible.flexible_records());
        assert!(TxnVersion::Verified.flexible_records());
        assert!(!TxnVersion::Flexible.verified());
        assert!(TxnVersion::Verified.verified());
    }
}
```

- [ ] **Step 3: Register the module**

In `crates/broker/src/txn/mod.rs`, add `pub(crate) mod version;`.

- [ ] **Step 4: Run + commit**

Run: `cargo test -p crabka-metadata finalized_feature && cargo test -p crabka-broker txn::version`
Expected: PASS.

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/metadata/src/image.rs crates/broker/src/txn/version.rs crates/broker/src/txn/mod.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(txn): finalized_feature accessor + TxnVersion resolver (KIP-890)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: TV_1 flexible `__transaction_state` record codec

**Files:**
- Modify: `crates/broker/src/txn/state.rs`
- Test: `crates/broker/src/txn/state.rs` (inline)

- [ ] **Step 1: Implement the flexible codec from Task 0's pinned schema**

Add two functions to `state.rs` that encode/decode `TxnEntry` to/from the **pinned Kafka `TransactionLogValue` flexible format** (Task 0, Step 2). Keep the existing `SerdeCompat<TxnEntry>` binary path as the TV_0 codec. Signatures:

```rust
impl TxnEntry {
    /// Encode as the Kafka flexible `TransactionLogValue` record (TV >= 1).
    /// Layout pinned from cp-kafka 4.0 in plan Task 0.
    pub(crate) fn encode_flexible(&self) -> Vec<u8> {
        // Implement against the pinned schema: value-version header + flexible
        // (tagged) fields for producer_id, producer_epoch, txn_timeout_ms,
        // state, partitions, offset_commit_groups, timestamps.
        todo!("implement from the Task 0 pinned TransactionLogValue schema")
    }

    /// Decode the Kafka flexible `TransactionLogValue` record (TV >= 1).
    pub(crate) fn decode_flexible(bytes: &[u8]) -> Result<Self, crate::error::BrokerError> {
        todo!("implement from the Task 0 pinned TransactionLogValue schema")
    }
}
```

> This is the one task whose body cannot be written verbatim ahead of Task 0 — the wire layout must come from the pinned schema, not memory. Replace the `todo!()`s with the real codec built directly against the bytes/field list recorded in Task 0. Reuse the protocol crate's flexible-field primitives (`crabka_protocol`'s tagged-field / varint helpers) rather than hand-rolling varints — grep `crates/protocol/src` for the existing flexible encode/decode utilities and use them.

- [ ] **Step 2: Round-trip test**

```rust
    #[test]
    fn flexible_round_trip() {
        let e = TxnEntry {
            transactional_id: "t1".into(),
            producer_id: 42,
            producer_epoch: 3,
            state: TxnState::Ongoing,
            txn_timeout_ms: 60_000,
            partitions: std::collections::HashSet::new(),
            offset_commit_groups: std::collections::HashSet::new(),
            last_update_ms: 1,
            start_ms: 0,
        };
        let bytes = e.encode_flexible();
        let back = TxnEntry::decode_flexible(&bytes).expect("decode");
        assert!(back == e);
    }
```

(Confirm `TxnEntry` derives `PartialEq`; add it if missing.)

- [ ] **Step 3: Cross-check against a real Kafka record (Task 0 sample)**

Add an assertion that `decode_flexible` accepts the exact value bytes captured from cp-kafka 4.0 in Task 0 (paste them as a `const SAMPLE: &[u8] = &[...]`). This proves byte-exact compatibility, not just self-consistency.

- [ ] **Step 4: Run + commit**

Run: `cargo test -p crabka-broker txn::state` → PASS.

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/txn/state.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(txn): flexible TransactionLogValue codec for transaction.version>=1 (KIP-890)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: TV_2 producer-epoch bump on completion

**Files:**
- Modify: `crates/broker/src/txn/handlers/end_txn.rs`
- Test: `crates/broker/src/txn/handlers/end_txn.rs` (inline) + covered by Task 7 integration

- [ ] **Step 1: Bump the epoch on completion at TV_2**

In `end_txn.rs`, after the transaction reaches `CompleteCommit`/`CompleteAbort` (the existing completion path), branch on the resolved TV. At TV_2, bump the entry's `producer_epoch` and return the bumped `producer_id`/`producer_epoch` in the EndTxn v5 response; below TV_2, keep the current behavior (no completion bump). Read the TV once at the top of `handle`:

```rust
    let image = broker.controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
```

At the completion site:

```rust
    if txnv.verified() {
        // KIP-890 TV_2: bump the producer epoch on every completion so a
        // hanging/zombie producer with the old epoch is fenced without a new
        // InitProducerId round-trip. Persist the bumped entry, then return the
        // new epoch in the response (EndTxn v5 producer_id/producer_epoch).
        let new_epoch = entry.producer_epoch.checked_add(1).unwrap_or(0);
        entry.producer_epoch = new_epoch;
        coord.put(entry.clone()).await?;
        // set resp.producer_id / resp.producer_epoch from the bumped entry
    }
```

> Implementer note: confirm the EndTxn v5 response struct carries `producer_id`/`producer_epoch` fields (check `crates/protocol/generated/EndTxnResponse.owned.rs`); if it does not at the advertised MAX (5), the response simply fences via the persisted bump and the next producer call sees `INVALID_PRODUCER_EPOCH`. Epoch-exhaustion (wraparound at i16::MAX) handling matches Kafka's "allocate a new producer_id" rule — pin from Task 0 and implement if reachable in tests; otherwise document as a follow-up.

- [ ] **Step 2: Unit test the bump decision**

Add a focused test that, given a `TxnVersion::Verified`, the completion path increments the epoch, and given `Classic`/`Flexible` it does not. Factor the bump decision into a small pure helper if the handler is hard to unit-test directly (e.g. `fn epoch_after_completion(txnv: TxnVersion, current: i16) -> i16`) and test that:

```rust
    #[test]
    fn epoch_bumps_only_at_tv2() {
        use crate::txn::version::TxnVersion;
        assert!(epoch_after_completion(TxnVersion::Classic, 3) == 3);
        assert!(epoch_after_completion(TxnVersion::Flexible, 3) == 3);
        assert!(epoch_after_completion(TxnVersion::Verified, 3) == 4);
    }
```

- [ ] **Step 3: Run + commit**

Run: `cargo test -p crabka-broker end_txn` → PASS. Confirm the existing `crates/broker/tests/transactions.rs::fenced_producer_cannot_commit` still passes (it asserts classic InitProducerId-reuse fencing, which is unchanged).

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/txn/handlers/end_txn.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(txn): TV_2 producer-epoch bump on transaction completion (KIP-890)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: TV_2 verify-only `AddPartitionsToTxn`

**Files:**
- Modify: `crates/broker/src/txn/handlers/add_partitions_to_txn.rs`
- Test: inline + Task 7 integration

- [ ] **Step 1: Honor `verify_only` at TV_2**

In the v4+ path (`process_one_txn`), branch on `TxnVersion`. At TV_2 with `transaction.verify_only == true`, do NOT add partitions — instead verify each requested partition is already part of the ongoing txn for `(producer_id, producer_epoch)`; return success for present partitions and the pinned abortable error (Task 0, Step 3 — expected `TRANSACTION_ABORTABLE`/`INVALID_TXN_STATE` `⟨pin⟩`) for absent ones. Below TV_2, `verify_only` is ignored and the classic explicit-add path runs (current behavior). Resolve TV at the top of `handle`:

```rust
    let image = broker.controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
```

In `process_one_txn`:

```rust
    if txnv.verified() && txn.verify_only {
        // KIP-890 TV_2: broker-internal verification — confirm the partitions
        // are already registered for this producer's ongoing txn; never add.
        return verify_partitions_present(&entry, &txn); // -> per-partition codes
    }
    // else: classic explicit-add path (unchanged).
```

- [ ] **Step 2: Unit test the verify decision**

Test `verify_partitions_present` (or the pure decision helper): a partition already in the entry → `NONE`; a partition not in the entry → the pinned abortable code; and that the verify branch is only taken when `txnv.verified() && verify_only`.

- [ ] **Step 3: Run + commit**

Run: `cargo test -p crabka-broker add_partitions_to_txn` → PASS.

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/txn/handlers/add_partitions_to_txn.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(txn): TV_2 verify-only AddPartitionsToTxn path (KIP-890)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Wire codec selection into the coordinator

**Files:**
- Modify: `crates/broker/src/txn/coordinator.rs`
- Test: `crates/broker/src/txn/coordinator.rs` (inline) + Task 7

- [ ] **Step 1: Select the codec by TV in `put`**

In `TxnCoordinator::put` (lines ~128-156), choose the value codec by the resolved TV. The coordinator can read the image via the `controller`/`partitions` it holds, or accept the `TxnVersion` as a parameter from the caller (preferred — the handlers already resolve it). Change `put`'s signature to take the resolved version:

```rust
    pub(crate) async fn put(&self, entry: TxnEntry, txnv: crate::txn::version::TxnVersion)
        -> Result<(), BrokerError>
    {
        // ...
        let payload = if txnv.flexible_records() {
            entry.encode_flexible()
        } else {
            <SerdeCompat<TxnEntry>>::serialize(&entry)?
        };
        // ... unchanged batch build / produce
    }
```

Update every `put` call site (the txn handlers) to pass the TV they resolved.

- [ ] **Step 2: Decode both formats in `recover`**

In `TxnCoordinator::recover` (lines ~166-234), the persisted records may be either format across a cluster's history. Detect the flexible value-version header (from Task 0's pinned format) and decode accordingly; fall back to the binary `SerdeCompat` decode for legacy/TV_0 records:

```rust
    let entry = if looks_flexible(value) {
        TxnEntry::decode_flexible(value)?
    } else {
        <SerdeCompat<TxnEntry>>::deserialize(value)?
    };
```

`looks_flexible` keys off the pinned value-version byte. Document the discriminator.

- [ ] **Step 3: Test mixed-format recovery**

Unit test: persist one entry via `encode_flexible` and one via the binary codec, run the recover-decode path over both, assert both reconstruct. (Construct the bytes directly; no live partition needed for the decode-dispatch helper if you factor `decode_value(value) -> TxnEntry` out.)

- [ ] **Step 4: Run + commit**

Run: `cargo test -p crabka-broker txn::coordinator` → PASS.

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/txn/coordinator.rs crates/broker/src/txn/handlers/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(txn): select __transaction_state codec by transaction.version; decode both on recover

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: Per-level behavior integration tests

**Files:**
- Create: `crates/broker/tests/transaction_version.rs`
- Modify: `crates/broker/tests/api_versions_features.rs` (advertise transaction.version)

- [ ] **Step 1: Advertise-surface assertion**

In `api_versions_features.rs`, assert `transaction.version` is advertised at `0..2` (mirror the `group.version` assertion added in the framework plan).

- [ ] **Step 2: Per-level behavior tests**

Create `crates/broker/tests/transaction_version.rs`. Mirror `crates/broker/tests/transactions.rs` for the harness (`boot_single`, `create_topic`, transactional produce/commit/abort). Three scenarios, each finalizing a TV via `UpdateFeatures` (or formatting at a release that defaults to it):

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
mod support;
// ... reuse transactions.rs helpers (extract shared ones into tests/fixtures if needed)

// 1) TV_2 (4.0 default): a full commit round-trip works AND the EndTxn
//    response / subsequent state reflects the bumped epoch (zombie with the
//    old epoch is fenced).
#[tokio::test]
async fn tv2_bumps_epoch_on_commit() { /* ... */ }

// 2) TV_1: commit round-trip works; the persisted __transaction_state record
//    is the flexible format (assert by recovering / dumping the value's
//    version header), and NO completion epoch bump occurs.
#[tokio::test]
async fn tv1_flexible_records_no_epoch_bump() { /* ... */ }

// 3) TV_0: classic behavior — binary records, no completion bump (regression
//    guard that the downlevel path still works).
#[tokio::test]
async fn tv0_classic_behavior() { /* ... */ }
```

> Implementer note: to put a cluster at TV_1/TV_0, downgrade `transaction.version` via `UpdateFeatures` (SAFE_DOWNGRADE) before producing, or boot a harness formatted at an older release. Confirm `support`/`transactions.rs` exposes enough to read back a persisted `__transaction_state` value; if not, assert behavior indirectly (epoch observed in EndTxn v5 response for TV_2 vs unchanged for TV_1).

- [ ] **Step 3: Existing txn suite stays green**

Run: `cargo test -p crabka-broker --test transactions --test transaction_version`
Expected: PASS — the 4.0-default harness runs TV_2, so `transactions.rs` exercises the new default path; all six existing tests stay green.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/tests/transaction_version.rs crates/broker/tests/api_versions_features.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(test): per-level transaction.version behavior + advertise surface (KIP-890)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 8: Full JVM acceptance sweep + docs (the compatibility gate)

**Files:**
- Modify: `STATUS.md`, `README.md`

- [ ] **Step 1: Workspace checks**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS.

- [ ] **Step 2: Full Docker jvm_acceptance sweep (re-baseline ALL advertised features)**

Run: `cargo test -p crabka-broker --test jvm_acceptance -- --ignored --test-threads=1`
Expected: every test green with `metadata.version` (25), `group.version` (1), and `transaction.version` (2) all advertised + finalized. Specifically verify: `kafka-features describe` lists all three at the expected levels; a kafka-clients 4.0 transactional producer commits end-to-end; the consumer engages KIP-848. This is the regression that previously took down 19 tests on a bad advertisement — it MUST be green.

- [ ] **Step 3: Triage any regression**

If a JVM test fails, identify the client/tool + Kafka version from the test name, and check whether it throws on the advertised *supported* level of a feature (the level is unknown to that client's enum) vs the *finalized* level. Smallest-blast-radius fix first; record the empirical finding in the relevant `*_version.rs` module doc and in `api_versions_features.rs`.

- [ ] **Step 4: Flip the docs**

Update `README.md`: KIP-584 row ⚠️ → ✅ (feature versioning now general + jvm-verified across all advertised features, closing the deferred metadata.version MAX=25 re-baseline), KIP-890 row reflects real `transaction.version` gating. Add the STATUS.md slice entry "Slice — transaction.version (KIP-890) downlevel behavior (2026-05-30)" with the TV_0/1/2 split, the codec selection, the epoch-bump/verify gates, and the jvm sweep result.

- [ ] **Step 5: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add STATUS.md README.md
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs: transaction.version (KIP-890) downlevel behavior + KIP-584 jvm re-baseline → ✅

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** transaction.version registration + per-release default + deps (Task 1) → TV resolver (Task 2) → TV_1 flexible records (Task 3, Task 6 wiring) → TV_2 epoch bump (Task 4) → TV_2 verify-only (Task 5) → per-level integration (Task 7) → full jvm gate + KIP-584 ✅ flip (Task 8). The spec's "coordinator behavior conditional on finalized TV" is realized by `resolve_txn_version` + the `flexible_records()`/`verified()` predicates threaded through the codec, EndTxn, and AddPartitionsToTxn.
- **Reality correction surfaced:** TV_2 behaviors are *implemented here*, not "made conditional" — Tasks 3/4/5 are new behavior; the existing path becomes TV_0. Flagged at top.
- **Deviations from spec (flagged):** transaction.version downgrade floor is the supported min (in-flight txn state is in the `__transaction_state` log, not the `MetadataImage`), mirroring group.version's deferral.
- **Placeholder honesty:** the only `todo!()` is Task 3's flexible codec body, which *cannot* be written before Task 0 pins the exact `TransactionLogValue` wire format — pinning is a required step, not a deferral. Every other step carries complete code.
- **Type consistency:** `TxnVersion` (`Classic`/`Flexible`/`Verified`) + `resolve_txn_version(&image)` + `flexible_records()`/`verified()`; `MetadataImage::finalized_feature(name) -> Option<i16>`; `TxnEntry::encode_flexible`/`decode_flexible`; `TxnCoordinator::put(entry, txnv)` — used consistently across Tasks 2–7.
- **Open implementer confirmations (flagged inline):** Task 0 pins the range/default/deps + the TransactionLogValue flexible schema + the verify abortable code; EndTxn v5 response epoch fields; `TxnEntry: PartialEq`; `put` call-site updates; harness support for reading persisted txn-state values.
```
