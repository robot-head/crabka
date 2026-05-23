# Crabka Operator — Slice 34: CA rotation orchestration (design)

**Date:** 2026-05-23
**Status:** Plan-ready
**Scope:** Turn the slice-30 *disruptive* CA-expiry path into a hands-off,
zero-downtime rotation. The cluster-CA (and clients-CA) cert Secret becomes a
multi-generation PEM trust bundle; the operator renews the CA cert (same key)
automatically on expiry, and performs a staged, coordinated key replacement on
demand — both leaning on slice-33 cert hot-reload + slice-21/28 ordered rolling
restart so inter-broker mTLS never drops.
**Depends on:** slice 30 (cluster/clients CA generation + per-broker keystore +
renewal CronJob), slice 33 (cert + truststore hot-reload), slice 21/28 (config-
hash-driven ordered rolling restart), slice 31 (per-listener TLS/SCRAM wiring).
**Closes:** Phase 4 (Security & certificate management).

## Goal

After slice 30, an operator-managed CA whose **cert** is within `renewalDays` of
`notAfter` is handled disruptively: the operator sets `CaRotationRequired=True`
and emits an Event, but does not rotate — an admin must replace the Secret pair
by hand, taking the mesh down. Slice 34 removes that cliff:

1. **Routine cert renewal (same key)** happens automatically. The CA cert is
   re-signed reusing the existing key with a fresh `validityDays`, the old cert
   is retained in the trust bundle until it expires, and a single ordered roll
   distributes the new bundle. Broker leaf certs are untouched (same key →
   same SPKI → existing leafs still chain), so the only cost is one safe roll.
2. **CA key replacement** (compromise, policy, or a forced rotation) runs a
   three-step coordinated dance — *distribute trust → promote key + reissue
   leafs → prune old trust* — each step a zero-downtime ordered roll, so the
   inter-broker mTLS mesh stays connected throughout.

The single visible behaviour change: a stock `Kafka` CR whose CA ages past
`renewalDays` rotates itself with no admin action and no broker downtime; an
admin can force either flavour of rotation with an annotation.

## Decisions captured during brainstorm

1. **Rotation lives in the reconciler, not the CronJob.** Rotation needs the
   rollout machinery (`combined_config_hash` → `plan_rollout` → per-pool
   `config-hash` label, gated on Ready). The CronJob can't roll pods. The
   slice-30 CronJob keeps doing *leaf* renewal (same-key, hot-reload, no roll)
   and BYO-expiry Events; it no longer sets the disruptive
   `CaRotationRequired` condition for operator-managed CAs — instead it nudges
   the reconciler (touches a `crabka.io/ca-renew-after` annotation) so the next
   reconcile picks up the rotation. The reconciler also detects expiry on its
   own 30 s requeue, so a missed CronJob run is not fatal.

2. **The trust bundle is the cert Secret's `ca.crt`, signing-cert-first.** No
   new Secret. `<cluster>-cluster-ca-cert/ca.crt` becomes a concatenation of
   one-or-more CA certs, **the active signing cert first**. This keeps every
   existing reader correct with no change: `cert_not_after` and rcgen's
   `Issuer::from_ca_cert_pem` both consume the first PEM block, and the broker's
   `RootCertStore` already loads *all* blocks from the file (slice 33). Steady
   state is a single cert — byte-identical to slice 30.

3. **The config-hash already hashes `ca.crt`.** `combined_config_hash` is fed
   `cluster_ca_outcome.material.cert_pem`, which is read from `ca.crt`. Making
   `ca.crt` a bundle means the hash now covers the *whole trust set* for free.
   Each rotation step deliberately rewrites the bundle string (append / reorder
   / drop a block), so each step flips the hash → one ordered roll. Routine
   leaf renewal never touches the bundle → no roll (slice-33 hot-reload). No
   canonicalisation needed: the reconciler preserves byte order when not
   rotating, and the deliberate reorder at *promote* is exactly the roll we
   want.

4. **Key-replacement is a 3-phase state machine, driven by Secret
   annotations + pool convergence — implemented as a pure planner.** Like
   `version::evaluate` and `logging::resolve_logging`, the decision logic is a
   pure function (`plan_ca_rotation`) over a reconstructed `CaState`; the
   reconciler only executes the returned `CaRotationPlan`. This keeps the hard
   part exhaustively unit-testable despite the FIFO integration mock.

5. **Triggers, Strimzi-shaped.** Annotations on the `Kafka` CR:
   `crabka.io/force-renew-ca` (force same-key cert renewal) and
   `crabka.io/force-replace-ca-key` (force key replacement). Consumed and
   removed once acted on, mirroring `strimzi.io/force-renew` /
   `strimzi.io/force-replace`. Auto-renewal needs no annotation.

6. **The staged key-replacement machine is cluster-CA only.** Both CAs get the
   multi-generation bundle, automatic same-key cert renewal, and auto-prune of
   expired trust anchors. The full three-phase *key replacement* (the part that
   needs roll-gated convergence + leaf reissue) is implemented for the **cluster
   CA** — the zero-downtime *inter-broker* mTLS path, which is the slice's
   raison d'être. Clients-CA *key* replacement is deferred: it additionally
   requires re-signing every `KafkaUser` mTLS cert (owned by the slice-37
   `KafkaUser` controller), and the clients-CA truststore is the data-plane
   listener path covered by slice-33 hot-reload rather than the inter-broker
   mesh. A `force-replace-ca-key` against the clients CA is refused with a
   `ClientsCaKeyReplaceUnsupported` condition reason + Warning Event.

7. **BYO CAs are never rotated.** `generateCertificateAuthority: false` keeps
   the slice-30 behaviour: the operator validates the pair, never overwrites,
   and the CronJob emits `ByoCaExpiringSoon` Events. No annotation has any
   effect on a BYO CA (a Warning Event explains why).

## Architecture

### Data model

`<cluster>-cluster-ca-cert` Secret (`type: Opaque`), the broker truststore:
- `ca.crt` — PEM **bundle**: `[signing_cert, …trust-only older certs]`.
- annotations:
  - `crabka.io/ca-cert-generation` — monotonic `u64`, bumped whenever the
    signing cert changes (renewal or key replacement).
  - `crabka.io/ca-rotation-phase` — `idle` | `key-replace-trust` |
    `key-replace-promote` (absent ≡ `idle`).

`<cluster>-cluster-ca` Secret, the signing material:
- `ca.key` — the **active** signing key (pairs with `ca.crt`'s first block).
- `ca.key.next` / `ca.crt.next` — present only during `key-replace-trust`: the
  staged new key + cert awaiting promotion.
- annotation `crabka.io/ca-key-generation` — monotonic `u64`, bumped on
  promotion.

The clients-CA pair (`-clients-ca-cert` / `-clients-ca`) carries the identical
shape.

**Invariant:** the first PEM block of `ca.crt` is the active signing cert and
pairs with `ca.key`. Every signing call uses `signing_cert(&bundle)` (the first
block); every truststore consumer uses the whole bundle.

### Trust-bundle helpers (`controller/cluster_ca.rs`)

Pure, no I/O:
- `split_pem_certs(bundle: &str) -> Vec<String>` — split into individual
  `-----BEGIN CERTIFICATE-----…` blocks (normalised, trailing newline each).
- `signing_cert(bundle: &str) -> &str` — first block (the signer).
- `join_bundle(blocks: &[String]) -> String` — concatenate.
- `prune_expired(blocks, now) -> Vec<String>` — drop blocks whose `notAfter` is
  past, but **never** the first (signing) block.
- `dedup_blocks(blocks) -> Vec<String>` — drop byte-duplicate certs, preserving
  first-seen order (idempotency guard so a re-reconcile doesn't grow the
  bundle).

### Security crate (`crabka_security::ca`)

Add same-key re-sign helpers (mirror `generate_cluster_ca` / `generate_clients_ca`,
but `KeyPair::from_pem(existing)` instead of generating a key):
- `renew_cluster_ca(key_pem: &str, cn: &str, validity_days: u32) -> Result<String /*cert_pem*/, CaError>`
- `renew_clients_ca(key_pem: &str, cn: &str, validity_days: u32) -> Result<String, CaError>`

The CN must match the original (`{cluster}-cluster-ca` / `{cluster}-clients-ca`)
and the cluster CA must re-emit `OU=cluster`, so the new cert's subject DN +
SPKI are identical to the old → existing leafs chain to it unchanged.

### Rotation planner (pure)

```rust
pub(crate) struct CaState {
    pub bundle: Vec<String>,           // signing-first
    pub key_pem: String,               // active signing key
    pub pending_key_pem: Option<String>,
    pub pending_cert_pem: Option<String>,
    pub cert_generation: u64,
    pub key_generation: u64,
    pub phase: CaPhase,                 // Idle | KeyReplaceTrust | KeyReplacePromote
}

pub(crate) struct RotationInputs<'a> {
    pub generate: bool,                 // generateCertificateAuthority
    pub validity_days: u32,
    pub renewal_days: u32,
    pub force_renew: bool,              // crabka.io/force-renew-ca present
    pub force_replace_key: bool,        // crabka.io/force-replace-ca-key present
    pub rollout_converged: bool,        // every pool carries the desired hash AND Ready
    pub now: OffsetDateTime,
    pub cn: &'a str,
}

pub(crate) enum CaRotationPlan {
    NoOp,
    RenewCertSameKey,                   // re-sign cert, reuse key
    StartKeyReplace,                    // generate new key+cert, stage + add new cert to bundle
    PromoteNewKey,                      // swap key, move new cert to front, reissue leafs
    PruneOldTrust,                      // drop superseded certs from bundle
}

pub(crate) fn plan_ca_rotation(state: &CaState, inp: &RotationInputs) -> CaRotationPlan;
```

Decision table (operator-managed CA only; BYO ⇒ always `NoOp`):

| phase | condition | plan |
|---|---|---|
| Idle | `force_replace_key` | `StartKeyReplace` |
| Idle | `force_renew` OR signing cert within `renewal_days` | `RenewCertSameKey` |
| Idle | bundle has a prunable (expired / superseded) non-signing block | `PruneOldTrust` |
| Idle | otherwise | `NoOp` |
| KeyReplaceTrust | `rollout_converged` | `PromoteNewKey` |
| KeyReplaceTrust | otherwise (roll still distributing trust) | `NoOp` |
| KeyReplacePromote | `rollout_converged` | `PruneOldTrust` (drops old cert, → Idle) |
| KeyReplacePromote | otherwise (roll applying new key) | `NoOp` |

`force_*` precedence: replace-key beats renew (a key replacement subsumes a
cert renewal). A `force_*` while a key replacement is mid-flight is ignored
(the in-flight phase wins) and re-surfaced via the status message.

### Executing a plan (`controller/cluster_ca.rs`)

`apply_ca_rotation(secret_api, kafka, which, state, plan, inp) -> RotationOutcome`:

- **RenewCertSameKey** — `new = renew_*_ca(state.key_pem, cn, validity)`;
  `bundle := dedup([new, …prune_expired(old, now)])`; bump
  `ca-cert-generation`; patch the cert Secret. Single roll (bundle changed).
- **StartKeyReplace** — `new = generate_*_ca(cn, validity)`; patch the key
  Secret with `ca.key.next`/`ca.crt.next` = new; patch the cert Secret:
  `bundle := dedup([old_signing, …rest, new.cert])` (new appended → trust-only),
  bump `ca-cert-generation`, set phase `key-replace-trust`. Roll distributes
  the larger trust set.
- **PromoteNewKey** — `ca.key := ca.key.next`; remove `*.next`; bump
  `ca-key-generation`; cert Secret `bundle := [new.cert, …old certs]` (new to
  front = new signing), set phase `key-replace-promote`. **Force-reissue every
  broker leaf** with the new key (see below). The bundle reorder flips the hash
  → roll restarts brokers onto the new-key leafs; the old cert is still trusted,
  so nothing breaks mid-roll.
- **PruneOldTrust** — `bundle := [signing] + non-expired,non-superseded rest`;
  if this leaves only the signing cert, set phase `idle`. Roll removes old-CA
  trust. (From `key-replace-promote` this is the terminal step → `idle`.)

`RotationOutcome` carries `force_reissue_leafs: bool` and the human-readable
status reason/message.

### Broker-leaf force reissue

`ensure_broker_keystore` today reissues a leaf only when its SAN digest
changed. Add a `force_reissue: bool` parameter: when `true` (promote step),
every requested broker leaf is re-signed with the current signing key
regardless of digest. The keystore Secret is not in the config-hash, so this
alone doesn't roll — the promote bundle-reorder provides the roll; brokers also
hot-reload the new leaf within the poll interval.

### Reconciler wiring (`controller/kafka.rs`)

Replace the slice-30 `ensure_cluster_ca`/`ensure_clients_ca` block with a
rotation-aware step per CA:

1. Load `CaState` from the two Secrets (bundle, key, pending, generations,
   phase). On first reconcile (no Secret) fall through to slice-30
   `ensure_ca` → generation 0, phase idle.
2. Compute `rollout_converged` from the same pool list `adopt_pools` uses
   (every pool's `config-hash` label == last-applied desired AND Ready). Since
   the desired hash depends on the *post-rotation* bundle, convergence is
   evaluated against the bundle currently in the Secret (pre-this-reconcile),
   which is exactly “did the previous step's roll finish”.
3. `plan = plan_ca_rotation(&state, &inputs)`; `outcome =
   apply_ca_rotation(...)`. Re-read the (possibly rewritten) bundle for the
   hash + signing material.
4. `combined_config_hash(..., Some(&trust_bundle_pem), ...)` — the bundle, not
   just the signing cert.
5. `ensure_broker_keystore(..., force_reissue = outcome.force_reissue_leafs)`,
   signing with `signing_cert(&trust_bundle_pem)` + active key.
6. Consume `force-*` annotations: once acted on, strip them via a metadata
   patch so they're one-shot.
7. Surface a `CaRotation` condition (and clear/replace the slice-30
   `CaRotationRequired`):
   - `False/Idle` — no rotation in flight.
   - `True/RenewingCert`, `True/DistributingTrust` (key-replace-trust),
     `True/PromotingKey` (key-replace-promote).
   - `False/<error>` for BYO-forced (`ByoCaImmutable`) or planner refusals.

The clients CA runs the same planner/executor but only ever yields `NoOp`,
`RenewCertSameKey`, or `PruneOldTrust` (single-step, no convergence gating):
its cert Secret is hot-reloaded by brokers (`client_ca_path`) and is not in the
config-hash, so same-key renewal needs no roll. `force-replace-ca-key` on the
clients CA is refused (`ClientsCaKeyReplaceUnsupported`). Only the cluster CA
drives the staged `StartKeyReplace`/`PromoteNewKey` machine and the config-hash.

### Status surface

Add to `KafkaStatus` (slice 34): per-CA `rotation` sub-object on the existing
`CertificateAuthorityStatus`:
```rust
pub struct CertificateAuthorityStatus {
    pub not_after: String,
    pub generated: bool,
    #[serde(default)] pub cert_generation: u64,   // slice 34
    #[serde(default)] pub key_generation: u64,    // slice 34
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_phase: Option<String>,           // slice 34: idle|distributing-trust|promoting-key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_anchors: Option<usize>,             // slice 34: cert count in the bundle
}
```

### CronJob / CLI

`ca-renewal-check` keeps renewing aging *leaf* certs (same-key, hot-reload) and
emitting BYO Events. The slice-30 `flag_ca_if_expiring` operator-managed branch
changes: instead of `CaRotationRequired=True`, it stamps a
`crabka.io/ca-renew-after=<rfc3339>` annotation on the `Kafka` CR (idempotent),
which wakes the reconciler to run `RenewCertSameKey`. BYO branch is unchanged.

### Helm

No new RBAC verbs (the operator already patches `kafkas`, `kafkas/status`,
`secrets`). The renewal `ClusterRole` is unchanged. `values.yaml` gains nothing
required; the reconciler owns rotation. The kind-e2e workflow gains a rotation
probe (below).

## Testing & validation strategy

**Unit (pure, no mock) — the bulk:**
- `crabka_security::ca`: `renew_cluster_ca`/`renew_clients_ca` reuse the key
  (same SPKI), keep subject DN (incl. `OU=cluster`), extend validity, and a
  leaf signed by the *old* cert verifies against the *renewed* cert.
- bundle helpers: split / signing / join round-trip; `prune_expired` keeps the
  signer; `dedup_blocks` is idempotent.
- `plan_ca_rotation`: every row of the decision table, both CAs, BYO ⇒ NoOp,
  force precedence, mid-flight force ignored, phase progression
  Idle→Trust→Promote→Prune→Idle gated on `rollout_converged`.
- `apply_ca_rotation` over an in-memory bundle: generation bumps, phase
  transitions, `force_reissue_leafs` set only at promote, pending staged then
  cleared.

**Integration (FIFO mock, single-reconcile observable transitions):**
1. CA cert within `renewalDays` ⇒ cert Secret patched with a 2-block bundle,
   `ca-cert-generation` 0→1, `CaRotation=True/RenewingCert`, broker leafs
   *not* reissued (same key).
2. `force-replace-ca-key` annotation ⇒ key Secret gains `*.next`, bundle grows
   to 2 blocks (old signing first), phase `key-replace-trust`,
   `CaRotation=True/DistributingTrust`, annotation stripped.
3. phase `key-replace-trust` + converged ⇒ key promoted, `*.next` cleared,
   bundle reordered (new first), leafs force-reissued, phase
   `key-replace-promote`.
4. phase `key-replace-promote` + converged ⇒ old cert pruned, bundle back to 1
   block, phase `idle`, `CaRotation=False/Idle`.
5. BYO + `force-replace-ca-key` ⇒ no Secret writes, `CaRotation=False/
   ByoCaImmutable`, Warning Event, annotation stripped.

**kind-e2e (`operator-e2e.yml`):** patch `demo` with
`crabka.io/force-replace-ca-key`, assert across reconciles that the
`-cluster-ca-cert` Secret transits 1→2→1 trust anchors, `ca-key-generation`
increments, the broker STS rolls, and the cluster returns to `Ready=True` with
`CaRotation=False/Idle` — i.e. a key replacement completed with the cluster
staying available.

## Out of scope (deferred)

- **Clients-CA key-replacement user-cert reissue** — mass re-signing of
  `KafkaUser` mTLS certs on a clients-CA *key* change (slice-37 follow-up). The
  trust-distribution half is in; the leaf-reissue half is not.
- **`MaintenanceTimeWindows`** — Strimzi gates rotation rolls to a cron window.
  Crabka rolls immediately; a maintenance-window gate is a later cross-cutting
  slice (also wanted by version upgrades).
- **PKCS#12 / JKS keystore output** — Crabka brokers consume PEM; no Java
  keystore artifacts.
- **CRL / OCSP** — revocation of a compromised-but-unexpired CA relies on
  prune-after-replace, not a revocation list.
- **Live key-replacement progress as Events per phase** beyond the
  `CaRotation` condition transitions.
