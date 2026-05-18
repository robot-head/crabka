# Crabka Operator Slice 36 — `KafkaUser` CRD: SCRAM-SHA-512 + ACLs (plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`. Per CLAUDE.md, dispatch tasks within a batch in parallel; sequential between batches. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Land the `KafkaUser` CRD with SCRAM-SHA-512 auth + simple ACL
authorization. Extends `crates/client-admin` with a `users` module and the
operator with `controller/user.rs` + finalizer-driven cleanup.

**Spec:** [`docs/superpowers/specs/2026-05-18-crabka-operator-kafkauser-36-design.md`](../specs/2026-05-18-crabka-operator-kafkauser-36-design.md).

**Tech stack:** Rust 2024, `kube-rs`, `k8s-openapi`, `schemars`,
`crabka-client-core`, `crabka-protocol`, `crabka-security` (PBKDF2),
`pbkdf2`, `sha2`, `rand` (salt generation).

---

## Batch overview

| Batch | Tasks | Files (disjoint within batch) | Parallel? |
|---|---|---|---|
| 1 | T1, T2, T3 | `crates/security/src/scram/mod.rs` ‖ `crates/client-admin/{Cargo.toml,src/users.rs,src/lib.rs,tests/users_round_trip.rs}` ‖ `crates/operator/src/crd/user.rs` + `crd/mod.rs` | yes |
| 2 | T4 | `crates/operator/src/controller/user.rs` + `controller/mod.rs` + `run.rs` + `gen_crds.rs` + `tests/reconcile_user.rs` | — |
| 3 | T5, T6 | `charts/crabka-operator/templates/clusterrole.yaml` ‖ `deploy/crds/crabka.io_kafkausers.yaml` (regen) | yes |

T4 depends on T1's `pbkdf2_salted` helper, T2's `AdminClient`/`AdminClientLike` extensions, and T3's `KafkaUser` type.

---

## Task 1 — `crabka-security`: expose `pbkdf2_salted`

**Files:** `crates/security/src/scram/mod.rs`

- [ ] **Step 1:** Add `pub fn pbkdf2_salted(password, mechanism, iterations, salt) -> Vec<u8>` returning the PBKDF2 output bytes for the requested SCRAM mechanism. Mechanism-aware (SHA-256 / SHA-512). Used by `crabka-client-admin` to compute the KIP-554 `salted_password` wire field.
- [ ] **Step 2:** Unit test that the new function's output matches the salt that `hash_scram_password_with_salt` consumes when re-computing `stored_key` / `server_key` via `derive_keys_from_salted`.

## Task 2 — `crates/client-admin/src/users.rs`

**Files:** `crates/client-admin/Cargo.toml`, `crates/client-admin/src/lib.rs`, `crates/client-admin/src/users.rs`, `crates/client-admin/tests/users_round_trip.rs`

- [ ] **Step 1:** `Cargo.toml` — add `crabka-security = { workspace = true }` and `rand = { workspace = true, features = ["std", "std_rng"] }`. (PBKDF2 itself comes via `crabka-security`.)
- [ ] **Step 2:** New `users.rs` module:
  - Local enum copies `ResourceType`, `PatternType`, `PermissionType`, `AclOperation` (Rust-typed; the wire `i8` is internal).
  - Structs `AclEntry`, `AclEntryFilter`, `ScramUpsertion`, `ScramDeletion`, `ScramUserOutcome`, `CreateAclOutcome`, `DeleteAclFilterOutcome`.
  - `impl AdminClient` block with `alter_user_scram_credentials_sha512`, `describe_acls`, `create_acls`, `delete_acls`. Each is a thin `Connection::send::<R>` wrapper that translates outcomes/errors.
  - `acl_to_creation(&AclEntry) -> AclCreation` and `acl_filter_to_wire(&AclEntryFilter) -> DeleteAclsFilter` pure helpers. Unit-tested.
- [ ] **Step 3:** Extend `AdminClientLike` in `lib.rs` with the four new methods. Add the corresponding `impl AdminClientLike for AdminClient` rows. Tests' fake admin will pick up the trait expansion in Task 4.
- [ ] **Step 4:** `tests/users_round_trip.rs` — spawn an in-process broker (mirrors `tests/round_trip.rs`), provision a super-user via bootstrap, then drive: upsert SCRAM → describe (none) → create ACL → describe (one) → delete ACL → describe (none) → delete SCRAM. Cleanup runs even if asserts fire.

## Task 3 — `crates/operator/src/crd/user.rs`

**Files:** `crates/operator/src/crd/user.rs`, `crates/operator/src/crd/mod.rs`

- [ ] **Step 1:** Define the CRD types:
  - `KafkaUser` derives `CustomResource` with `group=crabka.io`, `version=v1alpha1`, `kind=KafkaUser`, plural `kafkausers`, shortname `ku`, namespaced, `status = KafkaUserStatus`.
  - `KafkaUserSpec { authentication, authorization }`.
  - `Authentication` is a tagged enum on `type` with `ScramSha512(ScramSha512Auth)` for now. `ScramSha512Auth` holds `password_length` and `iterations` with sane defaults.
  - `Authorization` tagged on `type` with `Simple(SimpleAuthorization)`. `SimpleAuthorization` holds `acls: Vec<AclRule>`.
  - `AclRule { resource, operations, host, type }`. `AclResource { type, name, pattern_type }`. `AclOp` enum w/ all 11 Kafka operations. `AclType` enum (`allow|deny`). `PatternType` enum (`literal|prefixed`).
- [ ] **Step 2:** `KafkaUserStatus { conditions, observed_generation, username, secret, scram_sha512 }`.
- [ ] **Step 3:** Tests: CRD metadata (group / kind / plural / shortname), JSON round-trip with full spec, minimum-spec parse, optional-field omission.
- [ ] **Step 4:** `crd/mod.rs` — `pub mod user; pub use user::{KafkaUser, KafkaUserSpec, KafkaUserStatus, …};`.

## Task 4 — `crates/operator/src/controller/user.rs`

**Files:** `crates/operator/src/controller/user.rs`, `crates/operator/src/controller/mod.rs`, `crates/operator/src/run.rs`, `crates/operator/src/gen_crds.rs`, `crates/operator/tests/reconcile_user.rs`

- [ ] **Step 1:** New reconciler:
  - `run(ctx)` — `Controller::new(KafkaUser, _).watches(Kafka, …)` like topic reconciler.
  - `reconcile(obj, ctx)`:
    1. Cluster label → InvalidSpec on missing.
    2. Validate spec (mechanism, authorization kind, pattern types).
    3. Bootstrap address from `internal_listener_bootstrap` (re-used from `topic.rs`).
    4. Finalizer delete path: best-effort SCRAM delete + ACL principal-filter delete.
    5. Add finalizer if missing.
    6. Ensure password Secret (SSA, owner-ref). Reuse existing `password` key if Secret exists.
    7. Upsert SCRAM via `AdminClient`.
    8. `describe_acls(principal_filter = "User:<name>")` → diff against expanded spec → `create_acls(add)` + `delete_acls(remove)`.
    9. Patch status `Ready=True`.
  - Pure helpers `expand_spec_acls(&KafkaUserSpec, principal: &str)` and `diff_acls(current, desired)` returning `(additions, deletions)`.
- [ ] **Step 2:** `controller/mod.rs` — `pub mod user;`.
- [ ] **Step 3:** `run.rs` — spawn `controller::user::run(ctx)` alongside the others, add a `tokio::select!` arm.
- [ ] **Step 4:** `gen_crds.rs` — `write_one::<KafkaUser>(out_dir)?;` + assert the file is produced in the test.
- [ ] **Step 5:** `tests/reconcile_user.rs` — one happy-path reconcile test against `FakeAdminClient` (extended to record the new methods). Verifies Secret was created, ACLs converged, status set to Ready.

## Task 5 — RBAC

**Files:** `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1:** Add `kafkausers`, `kafkausers/status`, `kafkausers/finalizers` to the `crabka.io` rule. Existing `secrets` permission already covers the password Secret.

## Task 6 — Regenerate the CRD YAML

**Files:** `deploy/crds/crabka.io_kafkausers.yaml`

- [ ] **Step 1:** `cargo run -p crabka-operator --bin crabka-operator -- gen-crds --out-dir deploy/crds`. Check in the produced YAML.

---

## Acceptance

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `deploy/crds/crabka.io_kafkausers.yaml` regenerated and checked in
- Helm `ClusterRole` covers the new resource verbs
- Reconcile test passes the happy-path landing
