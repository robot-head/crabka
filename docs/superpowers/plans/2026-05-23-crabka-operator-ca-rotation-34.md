# Slice 34 — CA rotation orchestration — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 34 — Operator: CA rotation orchestration (2026-05-23)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Clients-CA key replacement is deferred (it additionally needs re-signing every KafkaUser mTLS cert, owned by the slice-37 controller); the clients CA gets the bundle + same-key renewal + auto-prune only

---

**Design:** [`docs/superpowers/specs/2026-05-23-crabka-operator-ca-rotation-34-design.md`]
**Branch:** `claude/next-slice-work-FAh6e`

## Acceptance gate

- `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` all green.
- CRD YAML regenerated (`cargo run -p crabka-operator -- gen-crds …` /
  existing regen path); `git diff` clean after regen.
- Operator lib unit tests + new `reconcile_ca_rotation` integration tests pass.
- Helm `helm template` / lint clean; kind-e2e rotation probe added.
- STATUS.md slice-34 entry.

## File sets (conflict map)

| Task | Files | Touches |
|---|---|---|
| T1 security | `crates/security/src/ca.rs` | renew fns + tests |
| T2 core | `crates/operator/src/controller/cluster_ca.rs`, `crates/operator/src/crd/ca.rs`, `crates/operator/src/crd/kafka.rs`, `crates/operator/src/controller/kafka.rs` | bundle helpers, planner, executor, keystore force-reissue, CronJob nudge, status, reconciler wiring |
| T3 tests | `crates/operator/tests/reconcile_ca_rotation.rs`, `crates/operator/tests/shared/mod.rs` (additive helpers only) | integration |
| T4 e2e | `.github/workflows/operator-e2e.yml` | kind rotation probe |
| T5 status+crd-yaml | `STATUS.md`, regenerated CRD YAML under `charts/`/`deploy/` | docs + manifests |

T1 is independent. T2 depends on T1's public API. T3/T4/T5 depend on T2.

## Batch 1 (parallel): T1

**T1 — `crabka_security::ca` same-key re-sign.**
Add:
```rust
pub fn renew_cluster_ca(key_pem: &str, cn: &str, validity_days: u32) -> Result<String, CaError>;
pub fn renew_clients_ca(key_pem: &str, cn: &str, validity_days: u32) -> Result<String, CaError>;
```
Body mirrors `generate_cluster_ca`/`generate_clients_ca` but uses
`KeyPair::from_pem(key_pem)?` instead of generating; returns `cert.pem()` only
(caller already holds the key). Cluster variant re-emits `OU=cluster`.
Tests: renewed cert has same SPKI as original (reuse key), same subject DN incl.
`OU=cluster`, later `notAfter`, and a leaf issued by the *original* cert verifies
against the *renewed* cert's public key.

## Batch 2 (after T1): T2 — operator rotation core

Implement in `controller/cluster_ca.rs` (pure + executor), `crd/ca.rs` +
`crd/kafka.rs` (status), `controller/kafka.rs` (wiring). Sub-steps:

1. **Bundle helpers** (pure): `split_pem_certs`, `signing_cert`, `join_bundle`,
   `prune_expired(now)` (never drops first block), `dedup_blocks`,
   `cert_not_after` already exists (reused for prune). Unit-test each.
2. **`CaState`, `CaPhase`, `RotationInputs`, `CaRotationPlan`** + pure
   `plan_ca_rotation`. Honour the design's decision table. Cluster CA: full
   machine. Clients CA flavour (`WhichCa::Clients`): refuse key replacement,
   only NoOp/RenewCertSameKey/PruneOldTrust. Unit-test every row.
3. **`load_ca_state(secret_api, cluster, which) -> Option<CaState>`** — read
   bundle (`ca.crt`), key (`ca.key`), `*.next`, generations + phase annotations.
   `None` ⇒ Secrets absent (first reconcile → fall back to slice-30 `ensure_ca`).
4. **`apply_ca_rotation(secret_api, kafka, which, state, plan, inp) ->
   RotationOutcome`** — execute the plan (patch cert + key Secrets, bump
   generations, set/clear phase annotation, stage/clear `*.next`). Returns
   `{ bundle_pem, signing_cert_pem, key_pem, force_reissue_leafs, cert_generation,
   key_generation, phase, reason, message }`.
5. **`ensure_broker_keystore` gains `force_reissue: bool`** — when true, reissue
   every requested leaf regardless of SAN digest. Update the single existing
   call site (kafka.rs) + thread the param.
6. **CronJob nudge:** `flag_ca_if_expiring` operator-managed branch stamps
   `crabka.io/ca-renew-after` annotation on the Kafka CR (idempotent: skip if
   already set within the window) instead of `CaRotationRequired=True`. BYO
   branch unchanged.
7. **Status:** extend `CertificateAuthorityStatus` with `cert_generation`,
   `key_generation`, `rotation_phase`, `trust_anchors` (all `#[serde(default)]`).
8. **Reconciler wiring (`kafka.rs`):** restructure the CA block per the design —
   move version + logging resolution + pool list ahead of CA; compute
   `current_hash` over the pre-rotation cluster-CA bundle; `rollout_converged`
   from pool labels+readiness; plan+apply rotation for cluster then clients;
   recompute `cfg_hash` over the post-rotation cluster-CA **bundle**; pass
   `force_reissue` to `ensure_broker_keystore`; sign leafs with
   `signing_cert(&bundle)`; consume + strip `force-*` and `ca-renew-after`
   annotations; emit `CaRotation` condition; populate the new status fields.
   Preserve the slice-30 BYO-missing early-return + the slice-24 empty-hash
   collapse behaviour for the non-rotating path.

Compile gate: `cargo clippy -p crabka-operator -p crabka-security --all-targets
-- -D warnings` + `cargo test -p crabka-operator -p crabka-security` (lib).

## Batch 3 (parallel, after T2 compiles): T3, T4, T5

**T3 — integration tests** (`tests/reconcile_ca_rotation.rs`, new): the five
single-reconcile scenarios from the design's testing section, using the FIFO
mock. Additive helpers in `shared/mod.rs` only (e.g. `fake_ca_cert_secret_with_anns`).

**T4 — kind-e2e rotation probe** (`operator-e2e.yml`): patch `demo` with
`crabka.io/force-replace-ca-key`; assert the `-cluster-ca-cert` trust-anchor
count transits 1→2→1, `ca-key-generation` increments, the STS rolls, and the
cluster ends `Ready=True` + `CaRotation=False/Idle`.

**T5 — STATUS.md + CRD YAML** — slice-34 entry (house style: bullets, dated
heading `## Slice 34 — Operator: CA rotation orchestration (2026-05-23)`);
regenerate CRD YAML so the new status fields land in the shipped manifests.

## Review

After Batch 3: full `cargo fmt --check` + `clippy -D warnings` + `cargo test
--workspace`; regen CRDs and confirm `git diff` only shows intended manifest
deltas; helm lint. Then commit, push, open draft PR.
