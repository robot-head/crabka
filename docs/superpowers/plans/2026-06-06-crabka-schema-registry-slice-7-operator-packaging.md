# Crabka Schema Registry Slice 7 — Operator CRD + Packaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `crabka-schema-registry` deployable on Kubernetes via a `SchemaRegistry` operator CRD + reconciler, a melange/apko container image, a Helm chart, docs, and the README capability flip.

**Architecture:** A new `crabka.io/v1alpha1` `SchemaRegistry` CRD (mirroring the `Kafka`/`KafkaTopic` kube-rs patterns) whose reconciler renders a stateless **Deployment** + a **headless Service** (per-pod DNS for slice-5 write-forwarding) + a **ClusterIP Service**, associated with a managed `Kafka` via the `crabka.io/cluster` label (bootstrap derived from the cluster's internal listener). SR is configured entirely through CLI flags/env (no config file), so the reconciler emits container args + mounts referenced Secrets. Packaging mirrors the broker (melange→apko OCI image, CI build-images loop). Validated by mock-client reconcile unit tests + a kind e2e schema round-trip.

**Tech Stack:** Rust, kube-rs (`CustomResource`, `Controller`, SSA), k8s-openapi, clap, melange/apko (Wolfi), Helm, kind, GitHub Actions.

---

## Conventions for every task (READ FIRST)

**Worktree + git safety (executors' shells reset cwd to the main repo — always use absolute paths / `git -C`):**
- Worktree: `/Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7`
- For git: `git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 ...`
- For cargo: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo ...`
- **Before every commit**, assert the branch: `git -C <worktree> rev-parse --abbrev-ref HEAD` MUST print `claude/schema-registry-slice-7`. If not, STOP.
- Commit (NEVER run `git config`): `git -C <worktree> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit ...`; commit-body trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Do NOT push** — the controller handles push/PR.

**Gate before every Rust commit** (from `<worktree>`):
- `cargo fmt`
- `cargo clippy --workspace --all-targets -- -D warnings` (touch changed files + re-run to defeat clippy's per-target cache; check the real exit code, not a `| grep`)
- `cargo test -p crabka-operator` (lib + the new reconcile test)

**The 5 wiring touch-points for a new operator CRD kind** (don't miss one):
1. `crates/operator/src/crd/mod.rs` — `pub mod schema_registry;` + re-export.
2. `crates/operator/src/gen_crds.rs` — import + `write_one::<SchemaRegistry>()` + regenerate `deploy/crds/` + test assertion.
3. `crates/operator/src/controller/mod.rs` — `pub mod schema_registry;`.
4. `crates/operator/src/run.rs` — spawn the controller + add a `select!` arm.
5. `crates/operator/src/config.rs` — `--default-schema-registry-image` flag → also update `tests/shared/mod.rs::op_config`.

**CRD YAML is generated, never hand-edited:** regenerate with `cargo run -p crabka-operator -- gen-crds deploy/crds`. CI `codegen-check.yml` runs `./tools/regenerate.sh && ./tools/regen-crds.sh` and fails if `git diff deploy/crds` is dirty — so always regenerate + commit the YAML.

**No printcolumns** — no existing CRD uses `printcolumn`; do not add any (keeps generated YAML consistent: `additionalPrinterColumns: []`).

---

## File Structure

**Batch 1 — Packaging** (file-disjoint from Batch 2)
- Create `packaging/melange/crabka-schema-registry.yaml` — melange recipe (cargo build the SR bin).
- Create `packaging/apko/crabka-schema-registry.yaml` — OCI image (nonroot, entrypoint).
- Modify `.github/workflows/operator-e2e.yml` — `changes` filter + `build-images` loop + artifact upload.

**Batch 2 — CRD** (file-disjoint from Batch 1)
- Create `crates/operator/src/crd/schema_registry.rs` — `SchemaRegistry{Spec,Status}` + nested types.
- Modify `crates/operator/src/crd/mod.rs` — module + re-export.
- Modify `crates/operator/src/gen_crds.rs` — register + test assertion.
- Create (generated) `deploy/crds/crabka.io_schemaregistries.yaml`.

**Batch 3 — Reconciler** (depends on Batch 2)
- Modify `crates/operator/src/config.rs` — `--default-schema-registry-image`.
- Create `crates/operator/src/controller/schema_registry.rs` — render fns + run/reconcile/error_policy.
- Modify `crates/operator/src/controller/mod.rs` — `pub mod schema_registry;`.
- Modify `crates/operator/src/run.rs` — spawn + select arm.
- Modify `crates/operator/tests/shared/mod.rs` — `op_config` new field + an SR CR builder + a ready-Kafka body helper if not reused.
- Create `crates/operator/tests/reconcile_schema_registry.rs` — mock-client reconcile unit tests.
- Modify `.github/workflows/ci.yml` — `operator-integration` llvm-cov job.
- Modify `codecov.yml` — `operator-integration` flag + bump `after_n_builds` 10→11.

**Batch 4 — Helm + RBAC** (depends on Batch 3)
- Create `charts/crabka-schema-registry/{Chart.yaml,values.yaml,.helmignore}` + `templates/{_helpers.tpl,deployment.yaml,service.yaml,service-headless.yaml,serviceaccount.yaml,NOTES.txt}`.
- Modify `charts/crabka-operator/templates/clusterrole.yaml` — add `schemaregistries` rule.

**Batch 5 — e2e + docs + README** (depends on Batch 3/4)
- Modify `.github/workflows/operator-e2e.yml` — `kind-schema-registry` job.
- Modify `crates/docgen/src/operator.rs` — `page::<SchemaRegistry>()` + bump assertions.
- Create `website/content/guide/deploying-schema-registry.md`.
- Modify `README.md:374` — `❌ → ✅`.

---

## Batch 1 — Packaging

### Task 1.1: melange + apko recipes

**Files:**
- Create: `packaging/melange/crabka-schema-registry.yaml`
- Create: `packaging/apko/crabka-schema-registry.yaml`

- [ ] **Step 1: Create the melange recipe** (clone of `packaging/melange/crabka-operator.yaml`, swapping the bin/crate/description). The image bundles `busybox` so a shell is available, matching the broker recipe.

`packaging/melange/crabka-schema-registry.yaml`:
```yaml
package:
  name: crabka-schema-registry
  version: 0.1.1
  epoch: 0
  description: Confluent Schema Registry-compatible service for Crabka
  copyright:
    - license: Apache-2.0

environment:
  contents:
    repositories:
      - https://packages.wolfi.dev/os
    keyring:
      - https://packages.wolfi.dev/os/wolfi-signing.rsa.pub
    packages:
      - ca-certificates-bundle
      - build-base
      - busybox
      - git
      - rustup

pipeline:
  - name: Install pinned Rust toolchain
    runs: |
      rustup-init -y --default-toolchain none --no-modify-path
      export PATH="$HOME/.cargo/bin:$PATH"
      rustup toolchain install 1.95.0 --component cargo --profile minimal
      rustup default 1.95.0
  - name: Build crabka-schema-registry
    runs: |
      export PATH="$HOME/.cargo/bin:$PATH"
      cargo build --release --bin crabka-schema-registry -p crabka-schema-registry
      install -D -m 0755 target/release/crabka-schema-registry "${{targets.contextdir}}/usr/bin/crabka-schema-registry"
```

- [ ] **Step 2: Create the apko image** (clone of `packaging/apko/crabka-broker.yaml` — keep `busybox` so the readiness `wget`/`sh` exists if needed; nonroot 65532).

`packaging/apko/crabka-schema-registry.yaml`:
```yaml
contents:
  repositories:
    - https://packages.wolfi.dev/os
  keyring:
    - https://packages.wolfi.dev/os/wolfi-signing.rsa.pub
  packages:
    - ca-certificates-bundle
    - tzdata
    - wolfi-baselayout
    - busybox
    - crabka-schema-registry

accounts:
  groups:
    - groupname: nonroot
      gid: 65532
  users:
    - username: nonroot
      uid: 65532
      gid: 65532
  run-as: 65532

entrypoint:
  command: /usr/bin/crabka-schema-registry

cmd: run

archs:
  - x86_64
```

- [ ] **Step 3: Validate YAML structure** (melange/apko aren't installed locally; the real build runs in CI). Confirm both files parse:

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && python3 -c "import yaml,sys; [yaml.safe_load(open(f)) for f in ['packaging/melange/crabka-schema-registry.yaml','packaging/apko/crabka-schema-registry.yaml']]; print('ok')"`
Expected: `ok`. Also eyeball: melange `package.name` (`crabka-schema-registry`) MUST equal the apko `packages:` entry; the apko `entrypoint.command` MUST equal the melange install path (`/usr/bin/crabka-schema-registry`).

- [ ] **Step 4: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add packaging/melange/crabka-schema-registry.yaml packaging/apko/crabka-schema-registry.yaml
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
operator/packaging: melange + apko recipes for crabka-schema-registry

Clone the broker/operator recipes for the SR binary: melange builds
`cargo build --release --bin crabka-schema-registry`, apko packages it as a
nonroot (65532) OCI image with entrypoint /usr/bin/crabka-schema-registry.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 1.2: CI build-images loop + path filter

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Add the SR crate path to the `changes` filter** so SR-only changes trigger the e2e. In the `changes` job's `filters.operator` list, add a line under the existing entries:
```yaml
              - 'crates/schema-registry/**'
              - 'charts/**'
```
(`charts/**` is added so the new SR chart also triggers; `helm/**` already exists but the charts live under `charts/`.)

- [ ] **Step 2: Add `crabka-schema-registry` to the `build-images` loop.** Change the loop line:
```yaml
          for recipe in crabka-operator crabka-broker; do
```
to:
```yaml
          for recipe in crabka-operator crabka-broker crabka-schema-registry; do
```

- [ ] **Step 3: Upload the SR image tarball.** In the `Upload image tarballs` step's `path:` list add:
```yaml
            crabka-schema-registry.tar
```

- [ ] **Step 4: Validate the workflow YAML parses.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && python3 -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 5: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add .github/workflows/operator-e2e.yml
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
ci(operator-e2e): build the crabka-schema-registry image

Add crabka-schema-registry to the build-images melange/apko loop + upload its
tarball, and add crates/schema-registry/** and charts/** to the e2e path filter.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — CRD (parallelizable with Batch 1)

### Task 2.1: `SchemaRegistry` CRD types

**Files:**
- Create: `crates/operator/src/crd/schema_registry.rs`
- Modify: `crates/operator/src/crd/mod.rs`

The CRD models exactly the slice-6 SR CLI surface (server-side security). SR↔broker client security is derived (slice 7 supports a PLAINTEXT internal listener; see Task 3.2). `bearer.mode` is `unsecured` only (JWKS deferred per spec §11). Credentials are referenced Secrets, never inline.

- [ ] **Step 1: Write `crates/operator/src/crd/schema_registry.rs`:**
```rust
//! `SchemaRegistry` CRD. Deploys the standalone `crabka-schema-registry`
//! service (a Kafka client of the broker; state lives in `_schemas`).
//! Stateless — N replicas join the `"sr"` election group, one is elected
//! primary, the rest forward writes. Associated with a managed `Kafka` via
//! the `crabka.io/cluster` label (mirrors `KafkaTopic`).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "SchemaRegistry",
    plural = "schemaregistries",
    singular = "schemaregistry",
    shortname = "sr",
    namespaced,
    status = "SchemaRegistryStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistrySpec {
    /// Stateless replicas; all join the election group. Default 1.
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Container image. Defaults to the operator's
    /// `--default-schema-registry-image`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Override bootstrap for an external/unmanaged Kafka. When unset,
    /// bootstrap is derived from the `crabka.io/cluster`-labeled Kafka's
    /// internal listener. (Secured external brokers are a future
    /// enhancement; the managed/label path is the secured one.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_servers: Option<String>,

    /// Backing compacted topic. Default `_schemas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schemas_topic: Option<String>,

    /// Replication factor for `_schemas` when auto-created. Default 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schemas_topic_replication_factor: Option<i32>,

    /// Election group id. Default `schema-registry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,

    /// Server TLS (HTTPS REST). None = plain HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<SchemaRegistryTls>,

    /// REST authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<SchemaRegistryAuthn>,

    /// REST authorization (Kafka-ACL based).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<SchemaRegistryAuthz>,

    /// Pod resource requirements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
}

/// Server TLS: cert/key (and optional client-cert verification) from Secrets.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTls {
    /// Secret (type kubernetes.io/tls) with `tls.crt` + `tls.key`.
    pub secret_name: String,
    /// Client-cert mode. Default `Disabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth: Option<TlsClientAuth>,
    /// Secret with `ca.crt` to verify client certs (required when
    /// `clientAuth` != Disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca_secret_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TlsClientAuth {
    Disabled,
    Optional,
    Required,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryAuthn {
    /// Reject anonymous requests with 401.
    #[serde(default)]
    pub require_auth: bool,
    /// `WWW-Authenticate: basic realm="<realm>"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic: Option<BasicAuthn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer: Option<BearerAuthn>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BasicAuthn {
    /// Secret with a single key holding newline-separated `user:cred`
    /// entries (cred = plaintext or `$2…` bcrypt). The key is mounted as
    /// a file and passed via `--basic-auth-file`.
    pub users_secret_name: String,
    /// Secret key holding the htpasswd-style file. Default `users`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub users_secret_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BearerAuthn {
    /// Bearer mode. Only `Unsecured` (dev) is supported today; JWKS is a
    /// future SR enhancement.
    pub mode: BearerMode,
    /// JWT claim used as the principal name. Default `sub`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_claim: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BearerMode {
    Unsecured,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryAuthz {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
    /// ACL-cache refresh interval (seconds). Default 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl_refresh_seconds: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryStatus {
    /// Kubernetes-style conditions: `KafkaReady`, `Available`, `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,
    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// In-cluster REST URL clients use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}
```

- [ ] **Step 2: Register the module in `crates/operator/src/crd/mod.rs`.** Add `pub mod schema_registry;` alphabetically after `pub mod rebalance;`, and add the re-export after the `rebalance::` re-export:
```rust
pub use schema_registry::{
    BasicAuthn, BearerAuthn, BearerMode, SchemaRegistry, SchemaRegistryAuthn, SchemaRegistryAuthz,
    SchemaRegistrySpec, SchemaRegistryStatus, SchemaRegistryTls, TlsClientAuth,
};
```

- [ ] **Step 3: Verify it compiles.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo build -p crabka-operator 2>&1 | tail -5`
Expected: `Finished`. (`k8s_openapi` is already a dep of crabka-operator — confirm with `grep k8s-openapi crates/operator/Cargo.toml`; it is used by `common.rs`.)

- [ ] **Step 4: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add crates/operator/src/crd/schema_registry.rs crates/operator/src/crd/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
operator: SchemaRegistry CRD types

crabka.io/v1alpha1 SchemaRegistry (shortname sr): replicas, image, Kafka
association (label + bootstrap override), schemas topic/RF/group, and the full
slice-6 server-side security surface (TLS+client-auth, basic/bearer authn,
authz super-users+refresh) with credentials as Secret refs. Status reuses
KafkaCondition. Bearer is unsecured-only (JWKS deferred).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 2.2: register in `gen_crds` + generate the manifest

**Files:**
- Modify: `crates/operator/src/gen_crds.rs`
- Create (generated): `deploy/crds/crabka.io_schemaregistries.yaml`

- [ ] **Step 1: Add the test assertion FIRST (TDD).** In `crates/operator/src/gen_crds.rs` `mod tests::writes_kafka_pool_topic_and_user_crd_files`, add:
```rust
        let sf = dir.path().join("crabka.io_schemaregistries.yaml");
        assert!(sf.exists());
        let sr = std::fs::read_to_string(&sf).unwrap();
        assert!(sr.contains("plural: schemaregistries"));
        assert!(sr.contains("- sr"));
```

- [ ] **Step 2: Run the test — verify it FAILS.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo test -p crabka-operator --lib gen_crds 2>&1 | tail -15`
Expected: FAIL (`sf.exists()` is false — `write_all` doesn't emit the SR CRD yet).

- [ ] **Step 3: Register the CRD.** In `gen_crds.rs`, add `SchemaRegistry` to the import:
```rust
use crate::crd::{Kafka, KafkaNodePool, KafkaRebalance, KafkaTopic, KafkaUser, SchemaRegistry};
```
and add to `write_all` (after the `KafkaRebalance` line):
```rust
    write_one::<SchemaRegistry>(out_dir)?;
```

- [ ] **Step 4: Run the test — verify it PASSES.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo test -p crabka-operator --lib gen_crds 2>&1 | tail -8`
Expected: PASS.

- [ ] **Step 5: Generate the committed manifest.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo run -p crabka-operator -- gen-crds deploy/crds 2>&1 | tail -8 && ls deploy/crds/crabka.io_schemaregistries.yaml`
Expected: prints `wrote .../crabka.io_schemaregistries.yaml` and the file exists. Sanity-check the header: `head -16 deploy/crds/crabka.io_schemaregistries.yaml` shows `name: schemaregistries.crabka.io`, `kind: SchemaRegistry`, `plural: schemaregistries`, `- sr`, `scope: Namespaced`.

- [ ] **Step 6: Gate + commit** (`cargo fmt`, clippy, test all green first).
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add crates/operator/src/gen_crds.rs deploy/crds/crabka.io_schemaregistries.yaml
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
operator: register SchemaRegistry in gen-crds + generated manifest

write_one::<SchemaRegistry> in write_all + the generated
deploy/crds/crabka.io_schemaregistries.yaml (codegen-check enforces no drift).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Reconciler (depends on Batch 2)

### Task 3.1: operator `--default-schema-registry-image` flag

**Files:**
- Modify: `crates/operator/src/config.rs`
- Modify: `crates/operator/tests/shared/mod.rs`

- [ ] **Step 1: Add the flag** to `OperatorConfig` in `config.rs`, after `default_broker_image`:
```rust
    /// Default schema-registry image used when `SchemaRegistry.spec.image` is unset.
    #[arg(long, env = "DEFAULT_SCHEMA_REGISTRY_IMAGE")]
    pub default_schema_registry_image: Option<String>,
```

- [ ] **Step 2: Update the test fixture** `op_config` in `crates/operator/tests/shared/mod.rs` (the struct literal has no `..Default::default()`), adding the field:
```rust
        default_schema_registry_image: None,
```
Also check `crates/operator/src/config.rs`'s own `#[cfg(test)] mod tests` (lines ~57–82) — if it constructs `OperatorConfig` with an explicit literal, add the field there too. (Grep: `rg -n "default_broker_image" crates/operator` and mirror every literal site.)

- [ ] **Step 3: Verify it compiles.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo build -p crabka-operator --all-targets 2>&1 | tail -5`
Expected: `Finished` (no "missing field" errors).

- [ ] **Step 4: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add crates/operator/src/config.rs crates/operator/tests/shared/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
operator: --default-schema-registry-image flag

Mirrors --default-broker-image; used as the SchemaRegistry pod image when
spec.image is unset. Update the op_config test fixtures.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 3.2: the reconciler + render helpers

**Files:**
- Create: `crates/operator/src/controller/schema_registry.rs`
- Modify: `crates/operator/src/controller/mod.rs`
- Modify: `crates/operator/src/run.rs`

**Design notes (read before writing):**
- Mirror `controller/topic.rs` for `run`/`error_policy`/`reconcile` structure, the `crabka.io/cluster` label read, the `kafka_api.get_opt(&cluster)` + `internal_listener_bootstrap` Ready-gate, and `Action::requeue`.
- Reuse from `controller::common`: `FIELD_MANAGER`, `apply_object`, generic `patch_status`, `condition`, `common_labels`, `owner_ref`. **`internal_listener_bootstrap` lives in `topic.rs`** — make it `pub(crate)` there (it already is) and import it: `use crate::controller::topic::internal_listener_bootstrap;`.
- There is **no Deployment/Service render in `common.rs`** — write SR-specific render fns in this file (use `serde_json::from_value(json!{...})` like `render_service`).
- SR is configured via **CLI args** (no config file) → render the container `args` vec; **no ConfigMap**. Credentials come from referenced Secrets (TLS cert, basic-users) mounted as volumes; the args point at the mount paths.
- **Slice 7 client-security scope:** support a **PLAINTEXT internal listener** (the default; the e2e broker is plaintext) → no `--kafka-*` args. Deriving/provisioning SASL/mTLS client creds for a secured internal listener is a noted follow-up. Bootstrap is still derived from the internal listener's `bootstrap_servers`.
- **advertised-url:** set via env `SCHEMA_REGISTRY_ADVERTISED_URL` using k8s dependent-env interpolation of `POD_NAME` (downward API): `"{scheme}://$(POD_NAME).{name}-sr-headless.{ns}.svc.cluster.local:8081"`. k8s expands `$(POD_NAME)` because `POD_NAME` is defined earlier in the same container's `env`.
- **Port:** 8081 (SR default `--listen-addr 0.0.0.0:8081`). Service/headless/probe all use 8081.
- **Readiness probe:** `GET /` on 8081 (returns `{}`); scheme `HTTPS` when `spec.tls` is set. With `require_auth` an unauthenticated `GET /` returns 401, so use a probe that treats any HTTP response as "up" — k8s `httpGet` treats 2xx/3xx as success, so set the probe to hit `/` and, when `require_auth` is true, rely on liveness via TCP socket instead. **Decision: use a `tcpSocket` readiness+liveness probe on 8081** (auth-agnostic, TLS-agnostic, mTLS-agnostic) to avoid the 401/scheme/client-cert complications. (An HTTP probe is a future refinement when require_auth is off.)

- [ ] **Step 1: Write the render helpers + reconciler** in `crates/operator/src/controller/schema_registry.rs`:
```rust
//! `SchemaRegistry` reconciler. Renders a stateless Deployment + headless
//! Service + ClusterIP Service for the `crabka-schema-registry` binary,
//! associated with a managed `Kafka` via the `crabka.io/cluster` label.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Service;
use kube::api::Api;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};
use serde_json::json;

use crate::context::Context;
use crate::controller::common::{
    ReconcileError, apply_object, common_labels, condition, owner_ref, patch_status,
};
use crate::controller::topic::internal_listener_bootstrap;
use crate::crd::{Kafka, SchemaRegistry, SchemaRegistryStatus, TlsClientAuth};

const APP_NAME: &str = "crabka-schema-registry";
const SR_PORT: i32 = 8081;
const DEFAULT_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-schema-registry:",
    env!("CARGO_PKG_VERSION")
);

pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let sr_api: Api<SchemaRegistry> = Api::all(ctx.client.clone());
    let kafka_api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(sr_api, watcher::Config::default())
        .watches(kafka_api, watcher::Config::default(), |_kafka| {
            Vec::<ObjectRef<SchemaRegistry>>::new().into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "schemaregistry reconciled"),
                Err(e) => tracing::warn!(error = %e, "schemaregistry reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<SchemaRegistry>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "schemaregistry reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

pub async fn reconcile(obj: Arc<SchemaRegistry>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let sr_api: Api<SchemaRegistry> = Api::namespaced(ctx.client.clone(), &ns);

    // 1. Cluster label (unless an explicit bootstrap override is set)
    let cluster = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned());

    // 2. Resolve bootstrap: spec override wins, else derive from the Kafka.
    let bootstrap = if let Some(b) = obj.spec.bootstrap_servers.clone() {
        Some(b)
    } else {
        let Some(cluster) = cluster.clone() else {
            set_status(&sr_api, &name, &obj, "MissingClusterLabel",
                "set metadata.labels[\"crabka.io/cluster\"] or spec.bootstrapServers", None, None).await?;
            return Ok(Action::requeue(Duration::from_secs(60)));
        };
        let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
        let kafka = kafka_api.get_opt(&cluster).await?;
        kafka.as_ref().and_then(internal_listener_bootstrap)
    };
    let Some(bootstrap) = bootstrap else {
        set_status(&sr_api, &name, &obj, "KafkaNotReady",
            "referenced Kafka is not Ready or has no internal listener", None, None).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    // 3. Render + apply children (Deployment + 2 Services).
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);

    let headless = render_headless_service(&obj)?;
    apply_object(&svc_api, &headless_name(&name), &headless).await?;
    let clusterip = render_clusterip_service(&obj)?;
    apply_object(&svc_api, &service_name(&name), &clusterip).await?;
    let image = obj.spec.image.clone()
        .or_else(|| ctx.config.default_schema_registry_image.clone())
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let deployment = render_deployment(&obj, &bootstrap, &image)?;
    apply_object(&dep_api, &deployment_name(&name), &deployment).await?;

    // 4. Status from the live Deployment.
    let live = dep_api.get_opt(&deployment_name(&name)).await?;
    let (replicas, ready) = live
        .as_ref()
        .and_then(|d| d.status.as_ref())
        .map_or((None, None), |s| (s.replicas, s.ready_replicas));
    let desired = obj.spec.replicas;
    let url = format!(
        "{}://{}.{}.svc.cluster.local:{SR_PORT}",
        scheme(&obj), service_name(&name), ns
    );
    if ready.unwrap_or(0) >= desired {
        set_status(&sr_api, &name, &obj, "Available",
            &format!("{desired} replica(s) ready"), Some((replicas, ready)), Some(url)).await?;
    } else {
        set_status(&sr_api, &name, &obj, "Progressing",
            &format!("{}/{desired} replica(s) ready", ready.unwrap_or(0)),
            Some((replicas, ready)), Some(url)).await?;
    }
    Ok(Action::requeue(Duration::from_secs(60)))
}

fn deployment_name(n: &str) -> String { format!("{n}-sr") }
fn service_name(n: &str) -> String { format!("{n}-sr") }
fn headless_name(n: &str) -> String { format!("{n}-sr-headless") }
fn scheme(obj: &SchemaRegistry) -> &'static str {
    if obj.spec.tls.is_some() { "https" } else { "http" }
}

fn sr_labels(obj: &SchemaRegistry) -> BTreeMap<String, String> {
    // Reuse the cluster-label convention; component label distinguishes SR.
    let name = obj.name_any();
    let mut m = common_labels(&name, "0.1.1", None);
    m.insert("app.kubernetes.io/name".into(), APP_NAME.into());
    m.insert("app.kubernetes.io/component".into(), "schema-registry".into());
    m
}

fn render_headless_service(obj: &SchemaRegistry) -> Result<Service, ReconcileError> {
    let name = obj.name_any();
    let labels = sr_labels(obj);
    let svc = serde_json::from_value(json!({
        "metadata": {
            "name": headless_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "clusterIP": "None",
            "selector": labels,
            "ports": [{ "name": "rest", "port": SR_PORT, "protocol": "TCP", "targetPort": SR_PORT }],
        }
    }))?;
    Ok(svc)
}

fn render_clusterip_service(obj: &SchemaRegistry) -> Result<Service, ReconcileError> {
    let name = obj.name_any();
    let labels = sr_labels(obj);
    let svc = serde_json::from_value(json!({
        "metadata": {
            "name": service_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "type": "ClusterIP",
            "selector": labels,
            "ports": [{ "name": "rest", "port": SR_PORT, "protocol": "TCP", "targetPort": SR_PORT }],
        }
    }))?;
    Ok(svc)
}

fn render_deployment(obj: &SchemaRegistry, bootstrap: &str, image: &str) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let ns = obj.meta().namespace.clone().unwrap_or_else(|| "default".into());
    let labels = sr_labels(obj);
    let (args, volumes, mounts) = build_args_and_mounts(obj, bootstrap);
    let advertised = format!(
        "{}://$(POD_NAME).{}.{}.svc.cluster.local:{SR_PORT}",
        scheme(obj), headless_name(&name), ns
    );
    let dep = serde_json::from_value(json!({
        "metadata": {
            "name": deployment_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "replicas": obj.spec.replicas,
            "selector": { "matchLabels": labels },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "volumes": volumes,
                    "containers": [{
                        "name": "schema-registry",
                        "image": image,
                        "args": args,
                        "env": [
                            { "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
                            { "name": "SCHEMA_REGISTRY_ADVERTISED_URL", "value": advertised },
                        ],
                        "ports": [{ "name": "rest", "containerPort": SR_PORT, "protocol": "TCP" }],
                        "volumeMounts": mounts,
                        "readinessProbe": { "tcpSocket": { "port": SR_PORT }, "initialDelaySeconds": 2, "periodSeconds": 5 },
                        "livenessProbe": { "tcpSocket": { "port": SR_PORT }, "initialDelaySeconds": 5, "periodSeconds": 10 },
                        "resources": obj.spec.resources.clone().unwrap_or_default(),
                    }],
                }
            }
        }
    }))?;
    Ok(dep)
}

/// Build the container args + the Secret volumes/mounts from the spec.
/// Non-secret config → args; credentials → mounted Secret files referenced
/// by path args. Returns (args, volumes, volumeMounts) as JSON values.
fn build_args_and_mounts(
    obj: &SchemaRegistry,
    bootstrap: &str,
) -> (Vec<String>, Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let s = &obj.spec;
    // The SR binary has no subcommand — args are flags only. (apko's
    // `cmd: run` default is replaced by the container `args` set here.)
    let mut a: Vec<String> = Vec::new();
    a.push(format!("--bootstrap-servers={bootstrap}"));
    a.push(format!("--listen-addr=0.0.0.0:{SR_PORT}"));
    if let Some(t) = &s.schemas_topic { a.push(format!("--schemas-topic={t}")); }
    if let Some(rf) = s.schemas_topic_replication_factor { a.push(format!("--schemas-topic-rf={rf}")); }
    if let Some(g) = &s.group_id { a.push(format!("--group-id={g}")); }

    let mut volumes = Vec::new();
    let mut mounts = Vec::new();

    // Server TLS
    if let Some(tls) = &s.tls {
        a.push("--tls-cert=/etc/sr/tls/tls.crt".into());
        a.push("--tls-key=/etc/sr/tls/tls.key".into());
        volumes.push(json!({ "name": "tls", "secret": { "secretName": tls.secret_name } }));
        mounts.push(json!({ "name": "tls", "mountPath": "/etc/sr/tls", "readOnly": true }));
        let mode = match tls.client_auth.unwrap_or(TlsClientAuth::Disabled) {
            TlsClientAuth::Disabled => "disabled",
            TlsClientAuth::Optional => "optional",
            TlsClientAuth::Required => "required",
        };
        a.push(format!("--tls-client-auth={mode}"));
        if let Some(ca) = &tls.client_ca_secret_name {
            a.push("--tls-client-ca=/etc/sr/client-ca/ca.crt".into());
            volumes.push(json!({ "name": "client-ca", "secret": { "secretName": ca } }));
            mounts.push(json!({ "name": "client-ca", "mountPath": "/etc/sr/client-ca", "readOnly": true }));
        }
    }

    // Authentication
    if let Some(authn) = &s.authentication {
        if authn.require_auth { a.push("--require-auth".into()); }
        if let Some(r) = &authn.realm { a.push(format!("--realm={r}")); }
        if let Some(b) = &authn.basic {
            let key = b.users_secret_key.clone().unwrap_or_else(|| "users".into());
            a.push("--basic-auth-file=/etc/sr/basic/users".into());
            volumes.push(json!({ "name": "basic", "secret": {
                "secretName": b.users_secret_name,
                "items": [{ "key": key, "path": "users" }]
            }}));
            mounts.push(json!({ "name": "basic", "mountPath": "/etc/sr/basic", "readOnly": true }));
        }
        if authn.bearer.is_some() {
            a.push("--bearer=unsecured".into());
            if let Some(pc) = authn.bearer.as_ref().and_then(|b| b.principal_claim.clone()) {
                a.push(format!("--bearer-principal-claim={pc}"));
            }
        }
    }

    // Authorization
    if let Some(az) = &s.authorization {
        if az.enabled { a.push("--authz".into()); }
        for u in &az.super_users { a.push(format!("--super-user={u}")); }
        if let Some(r) = az.acl_refresh_seconds { a.push(format!("--acl-refresh-secs={r}")); }
    }

    (a, volumes, mounts)
}

/// Patch status with a single rolled-up `Ready` condition + a `KafkaReady`
/// condition. `reason == "Available"` ⇒ Ready=True.
async fn set_status(
    api: &Api<SchemaRegistry>,
    name: &str,
    obj: &SchemaRegistry,
    reason: &str,
    message: &str,
    counts: Option<(Option<i32>, Option<i32>)>,
    url: Option<String>,
) -> Result<(), ReconcileError> {
    let kafka_ok = !matches!(reason, "MissingClusterLabel" | "KafkaNotReady");
    let ready = if reason == "Available" { "True" } else { "False" };
    let (replicas, ready_replicas) = counts.unwrap_or((None, None));
    let observed_generation = if ready == "True" { obj.meta().generation } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };
    let status = SchemaRegistryStatus {
        conditions: vec![
            condition("KafkaReady", if kafka_ok { "True" } else { "False" }, reason, message),
            condition("Ready", ready, reason, message),
        ],
        observed_generation,
        replicas,
        ready_replicas,
        url,
    };
    patch_status(api, name, status).await?;
    Ok(())
}
```

  **Implementation caveats to resolve while coding (don't leave as guesses):**
  - The `build_args_and_mounts` opener pushes then clears `"run"`; simplify to start with an empty `Vec` (the stub shows the reasoning — the SR binary has no subcommand). Final code: `let mut a: Vec<String> = Vec::new();`.
  - `common_labels` takes a `kafka_version` arg used only for the `app.kubernetes.io/version` label; passing `"0.1.1"` is acceptable, or drop the helper and build the label map inline. Ensure the **same** label map is used for Deployment `selector.matchLabels`, the pod template labels, and both Services' selectors (a selector/template mismatch = pods never become Ready). Selectors are immutable on a Deployment — keep them stable (only `app.kubernetes.io/{name,instance,component}`); do NOT put the version label in the selector. Refactor `sr_labels` into `selector_labels` (stable) + `meta_labels` (selector + version) accordingly.
  - Confirm `Secret` import is actually used (only if you create a Secret; this plan mounts referenced Secrets and creates none — remove the `Secret` import if unused to satisfy clippy).
  - `ResourceRequirements::default()` serializes to `{}` — fine.

- [ ] **Step 2: Register the controller module** in `crates/operator/src/controller/mod.rs`: add `pub mod schema_registry;` (alphabetically after `pub mod rebalance;`).

- [ ] **Step 3: Spawn the controller** in `crates/operator/src/run.rs`: after the `rebalance_handle` spawn, add:
```rust
    let schema_registry_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::schema_registry::run(ctx).await }
    });
```
and add a `select!` arm:
```rust
        res = schema_registry_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "SchemaRegistry controller exited with error"),
            Err(e) => tracing::error!(error = %e, "SchemaRegistry controller task panicked"),
        },
```

- [ ] **Step 4: Compile + gate.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo clippy -p crabka-operator --all-targets -- -D warnings 2>&1 | tail -20`
Expected: clean (resolve any unused-import / label-mismatch issues from the caveats).

- [ ] **Step 5: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add crates/operator/src/controller/schema_registry.rs crates/operator/src/controller/mod.rs crates/operator/src/run.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
operator: SchemaRegistry reconciler

Renders a stateless Deployment + headless Service (per-pod DNS for slice-5
write-forwarding via $(POD_NAME) advertised-url) + ClusterIP Service, gated on
the crabka.io/cluster Kafka being Ready (bootstrap from its internal listener;
PLAINTEXT internal listener for slice 7). CRD security fields render to SR CLI
args + mounted Secret volumes. Status: KafkaReady/Ready + replica counts + url.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 3.3: reconcile unit tests (mock-client harness)

**Files:**
- Create: `crates/operator/tests/reconcile_schema_registry.rs`
- Modify: `crates/operator/tests/shared/mod.rs` (add a ready-Kafka body + SR CR builder if not already shared)

- [ ] **Step 1: Write the test file** (mirror `reconcile_topic.rs`). It builds a `SchemaRegistry` CR, wires the FIFO mock transport with the GET-Kafka + GET-Deployment + apply/PATCH responses the reconciler will issue, calls `reconcile`, and asserts on the captured request bodies.

`crates/operator/tests/reconcile_schema_registry.rs`:
```rust
use assert2::assert;
use std::sync::Arc;

use crabka_operator::controller::schema_registry::reconcile;
use crabka_operator::crd::{SchemaRegistry, SchemaRegistrySpec};
use http::Method;
use kube::ResourceExt as _;

#[path = "shared/mod.rs"]
mod shared;
use shared::{MockRule, MockState, fixture_ctx, json_response, mock_client, not_found_body};

const NS: &str = "default";
const CLUSTER: &str = "demo";

fn sr(name: &str, cluster: Option<&str>) -> SchemaRegistry {
    let mut cr = SchemaRegistry::new(
        name,
        SchemaRegistrySpec {
            replicas: 1,
            image: None,
            bootstrap_servers: None,
            schemas_topic: None,
            schemas_topic_replication_factor: Some(1),
            group_id: None,
            tls: None,
            authentication: None,
            authorization: None,
            resources: None,
        },
    );
    cr.metadata.namespace = Some(NS.into());
    cr.metadata.uid = Some("uid-1".into());
    cr.metadata.generation = Some(1);
    if let Some(c) = cluster {
        cr.metadata.labels = Some([("crabka.io/cluster".to_string(), c.to_string())].into());
    }
    cr
}

/// A Ready Kafka body whose internal listener exposes a bootstrap address.
fn ready_kafka_body(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1", "kind": "Kafka",
        "metadata": { "name": name, "namespace": NS },
        "spec": { "kafkaVersion": "3.7.0" },
        "status": {
            "conditions": [{ "type": "Ready", "status": "True", "reason": "Ready",
                "message": "ok", "lastTransitionTime": "2026-01-01T00:00:00Z" }],
            "listeners": [{ "name": "PLAIN", "bootstrapServers": "demo-broker-headless.default.svc.cluster.local:9092" }]
        }
    })
}

#[tokio::test]
async fn missing_cluster_label_sets_status() {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/schemaregistries/sr1/status".into(),
        response: json_response(200, &serde_json::json!({
            "apiVersion": "crabka.io/v1alpha1", "kind": "SchemaRegistry",
            "metadata": { "name": "sr1", "namespace": NS }, "spec": { "replicas": 1 }
        })),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(sr("sr1", None)), ctx).await.unwrap();

    let observed = state.take_observed();
    let patch = observed.iter().find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status")).unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let ready = body["status"]["conditions"].as_array().unwrap().iter()
        .find(|c| c["type"] == "Ready").unwrap();
    assert!(ready["status"] == "False");
    assert!(ready["reason"] == "MissingClusterLabel");
}

#[tokio::test]
async fn renders_children_when_kafka_ready() {
    // FIFO: GET Kafka (ready) → apply headless svc → apply clusterip svc →
    // apply deployment → GET deployment (status) → PATCH status.
    let rules = vec![
        MockRule { method: Method::GET, path_substr: "/kafkas/demo".into(),
            response: json_response(200, &ready_kafka_body(CLUSTER)) },
        MockRule { method: Method::PATCH, path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}})) },
        MockRule { method: Method::PATCH, path_substr: "/services/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}})) },
        MockRule { method: Method::PATCH, path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":1}})) },
        MockRule { method: Method::GET, path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":1}})) },
        MockRule { method: Method::PATCH, path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})) },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(sr("sr1", Some(CLUSTER))), ctx).await.unwrap();

    let observed = state.take_observed();
    // The Deployment apply body carries the derived --bootstrap-servers arg.
    let dep = observed.iter().find(|r|
        r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")).unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let args = body["spec"]["template"]["spec"]["containers"][0]["args"].as_array().unwrap();
    let joined = args.iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" ");
    assert!(joined.contains("--bootstrap-servers=demo-broker-headless.default.svc.cluster.local:9092"));
    assert!(joined.contains("--schemas-topic-rf=1"));
    // advertised-url env uses $(POD_NAME) interpolation.
    let env = body["spec"]["template"]["spec"]["containers"][0]["env"].as_array().unwrap();
    let adv = env.iter().find(|e| e["name"] == "SCHEMA_REGISTRY_ADVERTISED_URL").unwrap();
    assert!(adv["value"].as_str().unwrap().contains("$(POD_NAME).sr1-sr-headless.default.svc.cluster.local:8081"));
    // Status rolled up to Ready/Available.
    let st = observed.iter().find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status")).unwrap();
    let sb: serde_json::Value = serde_json::from_slice(st.body()).unwrap();
    let ready = sb["status"]["conditions"].as_array().unwrap().iter().find(|c| c["type"] == "Ready").unwrap();
    assert!(ready["status"] == "True");
}

#[tokio::test]
async fn full_security_fields_render_to_args_and_mounts() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into()); // skip the Kafka GET
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: "sr-tls".into(),
        client_auth: Some(crabka_operator::crd::TlsClientAuth::Required),
        client_ca_secret_name: Some("sr-client-ca".into()),
    });
    cr.spec.authentication = Some(crabka_operator::crd::SchemaRegistryAuthn {
        require_auth: true, realm: Some("R".into()),
        basic: Some(crabka_operator::crd::BasicAuthn { users_secret_name: "sr-users".into(), users_secret_key: None }),
        bearer: None,
    });
    cr.spec.authorization = Some(crabka_operator::crd::SchemaRegistryAuthz {
        enabled: true, super_users: vec!["User:admin".into()], acl_refresh_seconds: Some(15),
    });
    // No Kafka GET rule needed (bootstrap override). Provide the apply/status rules.
    let rules = vec![
        MockRule { method: Method::PATCH, path_substr: "/services/sr1-sr-headless".into(), response: json_response(200, &serde_json::json!({"kind":"Service"})) },
        MockRule { method: Method::PATCH, path_substr: "/services/sr1-sr".into(), response: json_response(200, &serde_json::json!({"kind":"Service"})) },
        MockRule { method: Method::PATCH, path_substr: "/deployments/sr1-sr".into(), response: json_response(200, &serde_json::json!({"kind":"Deployment","status":{"replicas":1,"readyReplicas":0}})) },
        MockRule { method: Method::GET, path_substr: "/deployments/sr1-sr".into(), response: json_response(200, &serde_json::json!({"kind":"Deployment","status":{"replicas":1,"readyReplicas":0}})) },
        MockRule { method: Method::PATCH, path_substr: "/schemaregistries/sr1/status".into(), response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","spec":{"replicas":1}})) },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed.iter().find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")).unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let c = &body["spec"]["template"]["spec"]["containers"][0];
    let joined = c["args"].as_array().unwrap().iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" ");
    assert!(joined.contains("--tls-cert=/etc/sr/tls/tls.crt"));
    assert!(joined.contains("--tls-client-auth=required"));
    assert!(joined.contains("--tls-client-ca=/etc/sr/client-ca/ca.crt"));
    assert!(joined.contains("--require-auth"));
    assert!(joined.contains("--basic-auth-file=/etc/sr/basic/users"));
    assert!(joined.contains("--authz"));
    assert!(joined.contains("--super-user=User:admin"));
    assert!(joined.contains("--acl-refresh-secs=15"));
    // Mounts present for tls/client-ca/basic.
    let mounts = c["volumeMounts"].as_array().unwrap();
    let mount_paths: Vec<&str> = mounts.iter().map(|m| m["mountPath"].as_str().unwrap()).collect();
    assert!(mount_paths.contains(&"/etc/sr/tls"));
    assert!(mount_paths.contains(&"/etc/sr/client-ca"));
    assert!(mount_paths.contains(&"/etc/sr/basic"));
}
```

- [ ] **Step 2: Run the tests — verify they PASS** (the reconciler from Task 3.2 is already implemented; this is the verify-against-it step). If a test fails, the mismatch reveals a real bug — fix the reconciler, not the test.

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | tail -15`
Expected: `test result: ok. 3 passed`.

  Note on FIFO ordering: `mock_client` matches rules in registration order by `(method, path_substr)`. The reconciler issues requests in the order: GET kafka (if no override) → PATCH headless svc → PATCH clusterip svc → PATCH deployment → GET deployment → PATCH status. Keep the `rules` vec in that order. If kube issues an extra GET before an apply (it does not for SSA `Patch::Apply`), add the rule.

- [ ] **Step 3: Full gate** (`cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p crabka-operator`).

- [ ] **Step 4: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add crates/operator/tests/reconcile_schema_registry.rs crates/operator/tests/shared/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
operator: SchemaRegistry reconcile unit tests (mock-client)

Mirror reconcile_topic.rs: missing-label gate, children rendered when the Kafka
is Ready (derived --bootstrap-servers + $(POD_NAME) advertised-url + Ready
status), and full-typed-security fields rendering to the right args + Secret
mounts.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 3.4: CI coverage for the reconcile test

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `codecov.yml`

The `unit` flag is `--lib --bins` and never runs `tests/` binaries, so the reconcile test contributes zero codecov line coverage. Add a dedicated `operator-integration` job (mirroring `schema-registry-integration`).

- [ ] **Step 1: Add the job to `.github/workflows/ci.yml`** (after the `schema-registry-integration` job). The operator currently has many `reconcile_*.rs` tests with no coverage job — enumerate the SR one (others can be added later):
```yaml
  operator-integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.95.0"
      - uses: Swatinem/rust-cache@v2
        with:
          key: operator-integration
      - uses: taiki-e/install-action@cargo-llvm-cov
      - name: Operator integration coverage
        run: |
          mkdir -p coverage
          cargo llvm-cov -p crabka-operator --test reconcile_schema_registry --lcov --output-path coverage/operator-integration.lcov -- --nocapture
      - uses: codecov/codecov-action@v6
        with:
          token: ${{ secrets.CODECOV_TOKEN }}
          files: coverage/operator-integration.lcov
          disable_search: true
          flags: operator-integration
          fail_ci_if_error: false
```

- [ ] **Step 2: Register the flag in `codecov.yml`** — add under `flags:`:
```yaml
  operator-integration:
    carryforward: true
```
and bump both `after_n_builds: 10` → `11` (the `codecov.notify` and `comment` blocks).

- [ ] **Step 3: Validate both YAML files parse.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); yaml.safe_load(open('codecov.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add .github/workflows/ci.yml codecov.yml
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
ci: operator-integration coverage job + codecov flag

The unit flag (--lib --bins) never runs tests/ binaries, so reconcile tests
report zero coverage. Add an operator-integration llvm-cov job for
reconcile_schema_registry + the codecov flag; bump after_n_builds 10->11.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — Helm chart + operator RBAC (depends on Batch 3)

### Task 4.1: standalone `crabka-schema-registry` chart

**Files:**
- Create: `charts/crabka-schema-registry/Chart.yaml`, `values.yaml`, `.helmignore`
- Create: `charts/crabka-schema-registry/templates/{_helpers.tpl,deployment.yaml,service.yaml,service-headless.yaml,serviceaccount.yaml,NOTES.txt}`

This chart is the **non-operator** install path (points at an explicit `bootstrapServers`). It deploys the same Deployment/Service shape as the reconciler.

- [ ] **Step 1: `Chart.yaml`:**
```yaml
apiVersion: v2
name: crabka-schema-registry
description: Confluent Schema Registry-compatible service for Crabka
type: application
version: 0.1.0
appVersion: "0.1.1"
kubeVersion: ">= 1.28.0-0"
home: https://github.com/robot-head/crabka
sources:
  - https://github.com/robot-head/crabka
keywords:
  - kafka
  - crabka
  - schema-registry
maintainers:
  - name: The Crabka Authors
icon: https://raw.githubusercontent.com/robot-head/crabka/main/docs/crabka-square.png
```

- [ ] **Step 2: `values.yaml`:**
```yaml
image:
  repository: ghcr.io/robot-head/crabka-schema-registry
  tag: "0.1.1"
  pullPolicy: IfNotPresent

replicaCount: 1

# REQUIRED for the standalone chart: bootstrap address of the Crabka broker.
bootstrapServers: ""
schemasTopic: "_schemas"
schemasTopicReplicationFactor: 3
groupId: "schema-registry"

resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: "1000m"
    memory: 512Mi

nodeSelector: {}
tolerations: []
affinity: {}
podAnnotations: {}
podLabels: {}

serviceAccount:
  create: true
  name: ""

service:
  type: ClusterIP
  port: 8081

logFilter: "info"

securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile:
    type: RuntimeDefault

containerSecurityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: [ALL]
```

- [ ] **Step 3: `.helmignore`** (copy `charts/crabka-operator/.helmignore` verbatim).

- [ ] **Step 4: `templates/_helpers.tpl`** (clone `crabka-operator`'s, renaming `crabka-operator` → `crabka-schema-registry` and `component: operator` → `component: schema-registry`):
```
{{- define "crabka-schema-registry.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crabka-schema-registry.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "crabka-schema-registry.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "crabka-schema-registry.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "crabka-schema-registry.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "crabka-schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: schema-registry
{{- end -}}

{{- define "crabka-schema-registry.selectorLabels" -}}
app.kubernetes.io/name: {{ include "crabka-schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
```

- [ ] **Step 5: `templates/deployment.yaml`** (Deployment with the SR args; advertised-url via `$(POD_NAME)` + the headless service):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "crabka-schema-registry.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels: {{- include "crabka-schema-registry.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels: {{- include "crabka-schema-registry.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "crabka-schema-registry.selectorLabels" . | nindent 8 }}
        {{- with .Values.podLabels }}{{ toYaml . | nindent 8 }}{{- end }}
      annotations:
        {{- with .Values.podAnnotations }}{{ toYaml . | nindent 8 }}{{- end }}
    spec:
      serviceAccountName: {{ include "crabka-schema-registry.serviceAccountName" . }}
      securityContext: {{- toYaml .Values.securityContext | nindent 8 }}
      containers:
        - name: schema-registry
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          args:
            - --bootstrap-servers={{ required "bootstrapServers is required" .Values.bootstrapServers }}
            - --listen-addr=0.0.0.0:8081
            - --schemas-topic={{ .Values.schemasTopic }}
            - --schemas-topic-rf={{ .Values.schemasTopicReplicationFactor }}
            - --group-id={{ .Values.groupId }}
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: SCHEMA_REGISTRY_ADVERTISED_URL
              value: "http://$(POD_NAME).{{ include "crabka-schema-registry.fullname" . }}-headless.{{ .Release.Namespace }}.svc.cluster.local:8081"
            - name: RUST_LOG
              value: {{ .Values.logFilter | quote }}
          ports:
            - name: rest
              containerPort: 8081
              protocol: TCP
          readinessProbe:
            tcpSocket: { port: rest }
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            tcpSocket: { port: rest }
            initialDelaySeconds: 5
            periodSeconds: 10
          resources: {{- toYaml .Values.resources | nindent 12 }}
          securityContext: {{- toYaml .Values.containerSecurityContext | nindent 12 }}
      {{- with .Values.nodeSelector }}
      nodeSelector: {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations: {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity: {{- toYaml . | nindent 8 }}
      {{- end }}
```

- [ ] **Step 6: `templates/service.yaml`** (ClusterIP) and `templates/service-headless.yaml` (headless, for per-pod forwarding DNS):

`service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "crabka-schema-registry.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels: {{- include "crabka-schema-registry.labels" . | nindent 4 }}
spec:
  type: {{ .Values.service.type }}
  ports:
    - port: {{ .Values.service.port }}
      targetPort: rest
      protocol: TCP
      name: rest
  selector: {{- include "crabka-schema-registry.selectorLabels" . | nindent 4 }}
```
`service-headless.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "crabka-schema-registry.fullname" . }}-headless
  namespace: {{ .Release.Namespace }}
  labels: {{- include "crabka-schema-registry.labels" . | nindent 4 }}
spec:
  clusterIP: None
  ports:
    - port: 8081
      targetPort: rest
      protocol: TCP
      name: rest
  selector: {{- include "crabka-schema-registry.selectorLabels" . | nindent 4 }}
```

- [ ] **Step 7: `templates/serviceaccount.yaml`** (clone crabka-operator's, rename helper refs) and `templates/NOTES.txt`:
```
crabka-schema-registry is installed.

REST endpoint (in-cluster):
  http://{{ include "crabka-schema-registry.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.service.port }}

This chart requires `--set bootstrapServers=<broker-host:9092>`.
For an operator-managed deployment, apply a SchemaRegistry CR instead.
```

- [ ] **Step 8: Lint + render the chart.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && helm lint charts/crabka-schema-registry --set bootstrapServers=broker:9092 && helm template sr charts/crabka-schema-registry --set bootstrapServers=broker:9092 | python3 -c "import yaml,sys; list(yaml.safe_load_all(sys.stdin)); print('renders ok')"`
Expected: `1 chart(s) linted, 0 chart(s) failed` and `renders ok`. (If `helm` is unavailable locally, at minimum `python3 -c "import yaml; [yaml.safe_load(open(f)) for f in (...)]"` each template with the `{{ }}` lines is not valid YAML — so rely on `helm template`; note in the PR if helm wasn't available and CI must catch it. There is a `charts/crabka-rebalancer/tests/` helm-unittest precedent if you want unit specs, but that's optional for slice 7.)

- [ ] **Step 9: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add charts/crabka-schema-registry
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
charts: standalone crabka-schema-registry Helm chart

Non-operator install path: Deployment + ClusterIP + headless Service (for
slice-5 write-forwarding via $(POD_NAME) advertised-url), pointing at an
explicit --set bootstrapServers. nonroot 65532, tcpSocket probes on 8081.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 4.2: operator RBAC for `schemaregistries`

**Files:**
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1: Add the rule.** After the `kafkarebalances` rule in `clusterrole.yaml`, add (the operator already has `services`/`configmaps`/`secrets` + needs `deployments`):
```yaml
  - apiGroups: ["crabka.io"]
    resources: ["schemaregistries", "schemaregistries/status", "schemaregistries/finalizers"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```
And add `deployments` to the `apps` rule (it currently lists only `statefulsets`):
```yaml
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

- [ ] **Step 2: Render to confirm.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && helm template operator charts/crabka-operator | grep -A2 schemaregistries`
Expected: shows the new resource line. (If no helm, `python3 -c "import yaml;[yaml.safe_load(open('charts/crabka-operator/templates/clusterrole.yaml').read().replace('{{- if .Values.rbac.create -}}','').replace('{{- end }}',''))]"` won't fully parse templated YAML — rely on helm or visual review.)

- [ ] **Step 3: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add charts/crabka-operator/templates/clusterrole.yaml
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
charts(operator): RBAC for schemaregistries + deployments

The SchemaRegistry reconciler manages schemaregistries(/status,/finalizers) and
creates Deployments; grant both in the operator ClusterRole.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — e2e + docs + README (depends on Batch 3/4)

### Task 5.1: kind e2e — deploy SR + schema round-trip

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Add a `kind-schema-registry` job** mirroring the `kind` job's setup (load images → install CRDs → install operator chart → apply Kafka+pool), then apply a `SchemaRegistry` and do a REST round-trip. Add after the `kind` job:
```yaml
  kind-schema-registry:
    needs: [build-images, changes]
    if: ${{ github.event_name == 'push' || needs.changes.outputs.operator == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v6
      - uses: azure/setup-helm@v5
        with:
          version: v3.16.2
      - name: Download prebuilt image tarballs
        uses: actions/download-artifact@v8
        with:
          name: e2e-images
      - name: Create kind cluster
        uses: helm/kind-action@v1
        with:
          cluster_name: crabka-sr-e2e
          version: v0.24.0
          node_image: kindest/node:v1.30.0
      - name: Load images into kind
        run: |
          for img in crabka-operator crabka-broker crabka-schema-registry; do
            docker load -i "${img}.tar" 2>&1 | tee /tmp/load.log
            loaded=$(sed -n 's/^Loaded image: //p' /tmp/load.log | head -1)
            [ -n "$loaded" ] || { echo "::error::no image loaded for $img"; exit 1; }
            [ "$loaded" = "${img}:e2e" ] || docker tag "$loaded" "${img}:e2e"
            kind load docker-image "${img}:e2e" --name crabka-sr-e2e
          done
      - name: Install prometheus-operator CRDs
        run: |
          PROM_OP_TAG=v0.79.2
          kubectl apply -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml"
          kubectl apply -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"
      - name: Install CRDs
        run: |
          kubectl apply -f deploy/crds/crabka.io_kafkas.yaml
          kubectl apply -f deploy/crds/crabka.io_kafkanodepools.yaml
          kubectl apply -f deploy/crds/crabka.io_schemaregistries.yaml
      - name: Install operator chart
        run: |
          kubectl create namespace crabka-operator
          helm install operator charts/crabka-operator \
            --namespace crabka-operator \
            --set image.repository=crabka-operator --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
            --set brokerImage.repository=crabka-broker --set brokerImage.tag=e2e --set brokerImage.pullPolicy=IfNotPresent
          kubectl rollout status -n crabka-operator deploy/operator-crabka-operator --timeout=240s
      - name: Apply Kafka + KafkaNodePool
        run: |
          cat <<EOF | kubectl apply -f -
          apiVersion: crabka.io/v1alpha1
          kind: Kafka
          metadata: { name: demo, namespace: default }
          spec: { kafkaVersion: "3.7.0" }
          ---
          apiVersion: crabka.io/v1alpha1
          kind: KafkaNodePool
          metadata:
            name: brokers
            namespace: default
            labels: { crabka.io/cluster: demo }
          spec:
            roles: [Controller, Broker]
            replicas: 1
            nodeIdStart: 0
            storage: { type: PersistentClaim, size: 1Gi, deleteClaim: true }
          EOF
          kubectl wait --for=condition=Ready kafka/demo -n default --timeout=300s
      - name: Apply SchemaRegistry
        run: |
          cat <<EOF | kubectl apply -f -
          apiVersion: crabka.io/v1alpha1
          kind: SchemaRegistry
          metadata:
            name: sr
            namespace: default
            labels: { crabka.io/cluster: demo }
          spec:
            replicas: 1
            image: crabka-schema-registry:e2e
            schemasTopicReplicationFactor: 1
          EOF
          # Wait for the operator to roll the Deployment out.
          for i in $(seq 1 60); do
            kubectl get deploy sr-sr -n default >/dev/null 2>&1 && break || sleep 5
          done
          kubectl rollout status -n default deploy/sr-sr --timeout=300s
      - name: Register + fetch a schema (round-trip) via an in-cluster curl pod
        run: |
          BASE=http://sr-sr.default.svc.cluster.local:8081
          run_curl() { kubectl run sr-curl-$RANDOM -n default --image=curlimages/curl:8.10.1 --restart=Never --rm -i --quiet --command -- "$@"; }
          # POST a subject version.
          run_curl curl -sf -X POST "$BASE/subjects/orders-value/versions" \
            -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
            -d '{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"O\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}' | tee /tmp/reg.json
          grep -q '"id"' /tmp/reg.json
          # GET it back by version.
          run_curl curl -sf "$BASE/subjects/orders-value/versions/1" | tee /tmp/get.json
          grep -q 'orders-value' /tmp/get.json
      - name: Dump diagnostics on failure
        if: failure()
        run: |
          kubectl get all -n default
          kubectl describe schemaregistry sr -n default || true
          kubectl logs -n default deploy/sr-sr --all-containers --tail=200 || true
          kubectl logs -n crabka-operator deploy/operator-crabka-operator --tail=200 || true
```

- [ ] **Step 2: Validate the workflow YAML parses.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && python3 -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add .github/workflows/operator-e2e.yml
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
ci(operator-e2e): kind-schema-registry round-trip job

Deploy a SchemaRegistry CR on kind (operator-managed), wait for the Deployment,
then register + fetch an Avro schema over the in-cluster Service. Proof for the
README capability flip.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 5.2: docs (CRD reference page + guide)

**Files:**
- Modify: `crates/docgen/src/operator.rs`
- Create: `website/content/guide/deploying-schema-registry.md`

- [ ] **Step 1: Add the CRD to docgen's `crd_pages()`.** In `crates/docgen/src/operator.rs`, add `SchemaRegistry` to the `use crabka_operator::crd::{...}` import and `page::<SchemaRegistry>(),` to the vec; bump the test assertions in that file (`assert!(pages.len() == 5)` → `6`, and add the `schemaregistry` slug to the slug list `for e in [...]`). Find them: `rg -n "pages.len\\(\\) == 5|crd_pages|page::<" crates/docgen/src/operator.rs`.

- [ ] **Step 2: Verify docgen builds + the assertion passes.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && cargo test -p crabka-docgen 2>&1 | tail -8`
Expected: PASS.

- [ ] **Step 3: Write the guide page** `website/content/guide/deploying-schema-registry.md`:
```markdown
+++
title = "Deploying Schema Registry"
weight = 30
template = "docs/page.html"
+++

## Operator-managed (recommended)

Apply a `SchemaRegistry` next to a managed `Kafka`. The `crabka.io/cluster`
label binds it to the cluster; bootstrap is derived from the internal listener.

```yaml
apiVersion: crabka.io/v1alpha1
kind: SchemaRegistry
metadata:
  name: sr
  labels:
    crabka.io/cluster: demo
spec:
  replicas: 3
  schemasTopicReplicationFactor: 3
```

The operator creates a Deployment + a ClusterIP Service
(`sr-sr.<ns>.svc.cluster.local:8081`) + a headless Service for write
forwarding. See the [SchemaRegistry CRD reference](/reference/operator/schemaregistry/).

## Standalone (Helm, external broker)

```bash
helm install sr charts/crabka-schema-registry \
  --set bootstrapServers=my-broker:9092
```

## Security

`spec.tls`, `spec.authentication` (Basic / unsecured Bearer), and
`spec.authorization` (Kafka-ACL super-users) map to mounted Secrets + SR flags.
Credentials are always referenced Secrets, never inline.
```

- [ ] **Step 4: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add crates/docgen/src/operator.rs website/content/guide/deploying-schema-registry.md
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
docs: SchemaRegistry CRD reference + deploy guide

Add SchemaRegistry to docgen's crd_pages (auto-generates the CRD reference) +
a hand-written deploy guide (operator CR + standalone Helm).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

### Task 5.3: flip the README capability

**Files:**
- Modify: `README.md` (line 374)

- [ ] **Step 1: Flip the row.** Change `| Schema Registry | ❌ |` to `| Schema Registry | ✅ |`.

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 && grep -n "Schema Registry" README.md`
Expected: shows `| Schema Registry | ✅ |`.

- [ ] **Step 2: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 add README.md
git -C /Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-7 -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
docs(README): Schema Registry is implemented (flip the capability table)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Final verification (after all batches)

- [ ] `cd <worktree> && cargo fmt --all -- --check` — clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — clean (touch-force-relint to defeat the cache).
- [ ] `cargo test -p crabka-operator` — lib + `reconcile_schema_registry` green.
- [ ] `cargo test -p crabka-docgen` — green (the `crd_pages().len()` bump).
- [ ] `cargo run -p crabka-operator -- gen-crds deploy/crds` then `git -C <worktree> diff --exit-code deploy/crds` — **no diff** (codegen-check parity).
- [ ] `helm lint charts/crabka-schema-registry --set bootstrapServers=x:9092` — passes.
- [ ] All YAML (`operator-e2e.yml`, `ci.yml`, `codecov.yml`, both chart trees, both packaging recipes) parses.
- [ ] `git -C <worktree> log --oneline` shows the per-task commits on `claude/schema-registry-slice-7`.

---

## Execution batches (for the controller)

Dispatch by batch; tasks within a batch whose file sets don't overlap run in parallel:
- **Batch A (parallel):** Task 1.1, Task 1.2 (packaging + operator-e2e build-images) ‖ Task 2.1, Task 2.2 (CRD; 2.2 depends on 2.1 — sequential within the CRD lane). Packaging touches `packaging/**` + `operator-e2e.yml`; CRD touches `crates/operator/src/crd/**` + `gen_crds.rs` + `deploy/crds/**` — disjoint.
- **Batch B (sequential, depends on A):** Task 3.1 → 3.2 → 3.3 → 3.4 (all touch `crates/operator/**`; 3.2 depends on the CRD; 3.3 depends on 3.2; 3.4 is CI-only and could run parallel to 3.3).
- **Batch C (parallel, depends on B):** Task 4.1 (new chart) ‖ Task 4.2 (operator clusterrole) — disjoint files.
- **Batch D (parallel, depends on B/C):** Task 5.1 (operator-e2e e2e job) ‖ Task 5.2 (docgen + guide) ‖ Task 5.3 (README) — disjoint files. (5.1 edits `operator-e2e.yml`, which Task 1.2 also edited — but A is fully merged by the time D runs, so it's a sequential edit to different sections, no parallel conflict.)
