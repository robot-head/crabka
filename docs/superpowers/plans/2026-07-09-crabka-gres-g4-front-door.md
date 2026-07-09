# Chapter Gres G-4: Front Door Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tenants become a provisioned, routed, authenticated product: a compacted-topic registry drives per-tenant computes and an aggregated PgDog front door, from Kubernetes CRDs or a CLI.

**Architecture:** `crabka-gres-control` (registry client + tenant schema + PgDog renderers) is the control plane; the operator gains `Gres`/`GresTenant` CRDs and two controllers (per-tenant workloads; fleet aggregation + `RELOAD`); auth is PgDog passthrough with the tenant's SCRAM verifier stored in the registry and enforced by the compute; `crabka gres` CLI drives the same library.

**Tech Stack:** compacted-topic store (the `_schemas` idiom), `crabka-security` SCRAM primitives + a new `pg_authid` verifier codec, kube-derive CRDs + the operator's mock-harness test pattern, official PgDog image (pinned), `tokio-postgres` for PgDog admin `RELOAD`.

## Global Constraints

- **Prerequisites:** G-1/G-2 landed (G-3 recommended but only the registry's checkpoint-threshold fields depend on it). **Verify all quoted signatures against the landed tree** — the operator scaffolds, `crabka-security` SCRAM functions (`hash_scram_password`, `derive_keys_from_salted`, `ScramCredential`), and schema-registry's kafkastore reader/writer idioms were verified against today's tree; the gres crates against the G-1/G-2 plans.
- **Spec:** [2026-07-09-crabka-gres-g4-front-door-design.md](../specs/2026-07-09-crabka-gres-g4-front-door-design.md).
- **PgDog pin:** one image tag (latest stable at execution time, e.g. `ghcr.io/pgdogdev/pgdog:v0.1.x`) recorded in operator defaults and CI; bumping it is a deliberate commit gated by the e2e leg. PgDog is never vendored or patched.
- **Registry invariants:** `__gres_tenants` compacted, 1 partition; records are whole-tenant JSON snapshots keyed by tenant name with a monotonically bumped `record_version`; tombstone = deletion; verifiers only, never passwords.
- Lints/format/commit/test conventions as in the G-2 plan.

---

## Batch 1 — foundations (run Tasks 1 and 2 in parallel; disjoint crates)

### Task 1: `crabka-gres-control` — registry client + tenant schema

**Files:** Create `crates/gres-control/` (`Cargo.toml` internal-crate house style; deps: `crabka-client-core`, `crabka-client-admin`, `crabka-protocol`, `serde`, `serde_json`, `thiserror`, `tokio`, `tracing`; dev: `assert2`, `proptest`, `crabka-broker`, `crabka-client-producer`, `tempfile`), `src/{lib,error,record,registry}.rs`, `README.md`; add the release-plz internal entry.

**Interfaces:**
- Produces:
```rust
pub const TENANT_REGISTRY_TOPIC: &str = "__gres_tenants";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantRecord {
    pub record_version: u64,
    pub name: String,
    pub state: TenantState,                 // Active | Suspended (+ ResumeRequested lands in G-5)
    pub sql_user: String,
    pub scram_verifier: String,             // pg_authid format (Task 2's codec)
    pub wal_replication: i32,
    pub bucket_prefix: Option<String>,
    pub checkpoint_frames: Option<u64>,
    pub checkpoint_bytes: Option<u64>,
    pub idle_seconds: Option<u64>,          // schema now; semantics in G-5 (0/None = never)
}

pub struct Registry { /* bootstrap, producer (idempotent, acks=all), reader conn */ }
impl Registry {
    pub async fn connect(bootstrap: &str) -> Result<Self, ControlError>;
    pub async fn ensure_topic(&mut self, replicas: i32) -> Result<(), ControlError>;   // compact, 1 partition
    pub async fn upsert(&mut self, rec: &TenantRecord) -> Result<(), ControlError>;    // produce + await own offset applied
    pub async fn delete(&mut self, tenant: &str) -> Result<(), ControlError>;          // tombstone
    pub async fn get(&mut self, tenant: &str) -> Result<Option<TenantRecord>, ControlError>;
    pub async fn list(&mut self) -> Result<Vec<TenantRecord>, ControlError>;
    pub fn watch(&self) -> tokio::sync::watch::Receiver<u64>;                          // last-applied offset (the _schemas idiom)
}
pub fn fold(records: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>) -> BTreeMap<String, TenantRecord>; // pure, tombstone-aware
```
- Implementation mirrors `crates/schema-registry/src/kafkastore/{topic,writer,reader}.rs` (create-with `cleanup.policy=compact` tolerating 36; group-less tail from offset 0 via `client_core::fetch_partition`; last-applied `watch`); `record_version` conflicts resolve highest-wins in `fold` (ties: last record wins).

Steps: failing pure-`fold` unit tests (create/update/suspend/tombstone orderings, version conflicts) + proptest (fold is order-insensitive for distinct versions); implement record + fold; failing integration (in-process broker: ensure → upsert → get read-your-writes → list → delete); implement registry; nextest/clippy/fmt/README; commit `feat(gres): tenant registry over a compacted topic`.

### Task 2: `pg_authid` verifier codec in `crabka-security`

**Files:** Create `crates/security/src/scram/pg_verifier.rs` (+ module wiring), tests inline + a fixture test.

**Interfaces:**
- Produces:
```rust
/// `SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>`
pub struct PgScramVerifier { pub iterations: u32, pub salt: Vec<u8>, pub stored_key: Vec<u8>, pub server_key: Vec<u8> }
impl PgScramVerifier {
    pub fn generate(password: &str, iterations: u32) -> Self;          // via hash_scram_password(.., ScramSha256, ..)
    pub fn parse(s: &str) -> Result<Self, ScramError>;
    pub fn to_string(&self) -> String;                                  // Display impl
}
```
Steps: failing tests — round-trip; parse of a **fixture generated by real PostgreSQL** (obtain once via `docker run postgres:18` + `CREATE ROLE fixture PASSWORD 'hunter2'` + `SELECT rolpassword FROM pg_authid` — check the literal string into the test with a comment recording how it was produced); reject SHA-512/garbage inputs. Implement over the existing primitives (`hash_scram_password_with_salt` for deterministic tests). Cross-check: verify `generate`'s keys validate a real SCRAM exchange via `ScramServerExchange` (mechanism pinned SHA-256). nextest/clippy/fmt; commit `feat(security): pg_authid SCRAM verifier codec`.

---

## Batch 2 — consumption (run Tasks 3, 4, 5 in parallel; disjoint crates)

### Task 3: PgDog config renderers in `crabka-gres-control`

**Files:** Create `crates/gres-control/src/pgdog.rs` (+ module wiring), golden fixtures under `crates/gres-control/tests/golden/`.

**Interfaces:**
- Produces:
```rust
pub struct PgdogRenderInput<'a> {
    pub tenants: &'a [TenantEndpoint],   // name, backend host, backend port, state
    pub activator: Option<(String, u16)>, // suspended tenants route here (G-5; None routes them nowhere = omitted)
    pub general: PgdogGeneral,            // listen port, tls paths, passthrough_auth, pooler_mode, admin password secret ref
}
pub fn render_pgdog_toml(input: &PgdogRenderInput) -> String;
pub fn render_users_toml(input: &PgdogRenderInput) -> String; // passthrough mode: minimal skeleton; dev mode: per-tenant users
```
Rendered through typed serde structs → `toml` (add `toml` to workspace deps if absent — check first; the operator/broker config stacks likely already carry it), one `[[databases]]` entry per active tenant (`name`, `host`, `port`, `pooler_mode`), `passthrough_auth = "enabled"` in `[general]`.

Steps: failing golden tests (two-tenant render byte-compares against checked-in goldens; suspended tenant with activator renders the activator endpoint; dev-mode users.toml variant), implement, a documented **pin-upgrade check** note in the module docs (goldens re-validated against the pinned PgDog by the e2e leg), nextest/clippy/fmt, commit `feat(gres): PgDog config renderers`.

### Task 4: `crabka-pgwire` accepts registry verifiers

**Files:** Modify `crates/pgwire/src/scram.rs` (or `session.rs`) — a constructor `ScramVerifier::from_pg_verifier(parsed: &PgScramVerifier) -> Self` equivalent (field mapping: iterations/salt/stored_key/server_key; check the vendored struct's actual fields and add `crabka-security` as a dependency of `crabka-pgwire` **only if** the types don't line up as a plain data mapping — prefer a From impl in `gres-control` or `gres` to keep pgwire dependency-light; decide by inspecting the landed `ScramVerifier` fields and keep pgwire zero-new-deps if possible).

Steps: failing test — a session config built from a `PgScramVerifier::generate("hunter2", 4096)` authenticates a `tokio-postgres` SCRAM connection end-to-end (mirror the vendored `scram_auth.rs` test's harness); implement the mapping; nextest/clippy/fmt; commit `feat(pgwire): construct verifiers from pg_authid material` (or land the mapping in gres-control with the same test if pgwire stays untouched).

### Task 5: Compute boots from the registry

**Files:** Modify `crates/gres/src/main.rs`, `Cargo.toml` (add `crabka-gres-control`).

Substrate mode gains: read own `TenantRecord` at boot (`Registry::connect` + `get(tenant)`; absent record → clear startup error naming `crabka gres create-tenant`); apply `sql_user` + `scram_verifier` → SCRAM `SessionConfig` (auth defaults to scram in substrate mode; `--auth trust` remains a dev override), `bucket_prefix`/thresholds → checkpointer config (explicit flags still override, documented as dev knobs). Steps: failing integration test (harness provisions a record via `Registry`, boots the compute, `tokio-postgres` connects with SCRAM using the tenant password, trust-mode connection refused); implement; nextest/clippy/fmt; update `crates/gres/README.md`; commit `feat(gres): compute self-configures from the tenant registry`.

---

## Batch 3 — operator kinds (serial: shared files `crd/mod.rs`, `gen_crds.rs`, `controller/mod.rs`, `run.rs`)

### Task 6: `Gres` + `GresTenant` CRDs and registration

**Files:** Create `crates/operator/src/crd/gres.rs`, `src/crd/gres_tenant.rs`; modify `src/crd/mod.rs`, `src/gen_crds.rs` (+ its test table), `src/controller/mod.rs` (stub modules), `src/run.rs` (spawn arms), `src/config.rs` (`default_gres_image`, `default_pgdog_image`); run `tools/regen-crds.sh`; sample YAML under `crates/operator/sample/`.

**Interfaces (spec structs, kube-derive `crabka.io/v1alpha1`, shortnames `gg`/`gt`):**
```rust
pub struct GresSpec {          // kind Gres — the fleet
    pub kafka_cluster: String,             // crabka.io/cluster target
    pub pgdog: PgdogSpec,                  // image (default from config), replicas, listen port, tls secret ref, admin secret ref
    pub defaults: Option<TenantDefaults>,  // wal_replication, checkpoint thresholds, idle_seconds
}
pub struct GresTenantSpec {    // kind GresTenant — one tenant
    pub gres: String,                      // fleet name
    pub user: String,
    pub password_secret_ref: SecretKeyRef, // hashed to a verifier at reconcile; never stored in the CR/status
    pub suspended: Option<bool>,
    pub resources: Option<ResourceRequirements>,
    pub overrides: Option<TenantDefaults>,
}
```
Status structs carry observed state (`ready`, `walTopic`, `registryVersion`, `lastCheckpointOffset`). Steps: structs + regen (CRD YAML diff committed) + `gen_crds` test row + empty controllers registered; workspace builds; commit `feat(operator): Gres and GresTenant CRDs`.

### Task 7: The `GresTenant` controller

**Files:** Create `crates/operator/src/controller/gres_tenant.rs`, `crates/operator/tests/reconcile_gres_tenant.rs` (mock harness).

Reconcile (the KafkaGrpcGateway shape): resolve fleet + kafka cluster by label → ensure `__gres_wal.<t>` (admin via the cached `AdminClientLike` seam so tests fake it) → read password Secret → build verifier (Task 2) → `Registry::upsert` (registry access behind a new `RegistryLike` trait on `Context`, faked in tests like `fake_admin`) → SSA compute Deployment + Service (env: bootstrap, tenant; image from config; the `crabka.io/cluster` label) → patch status. Deletion (finalizer): tombstone + workload GC. Steps: mock-harness tests first (exact request sequences per the house pattern: create path, suspend flag flips the registry record, delete path), implement, nextest/clippy/fmt, commit `feat(operator): GresTenant reconciler`.

### Task 8: The `Gres` (fleet) controller

**Files:** Create `crates/operator/src/controller/gres.rs`, `crates/operator/tests/reconcile_gres.rs`.

Reconcile: list `GresTenant`s of the fleet → render `pgdog.toml`/`users.toml` (Task 3) into a config Secret (SSA, single owner — the aggregation pattern) → SSA PgDog Deployment (pinned image, config Secret mounted 0400, OpenMetrics port annotated) + Service → if the config hash changed: `RELOAD` via `tokio-postgres` to the PgDog admin database (admin credentials from the fleet's admin secret ref; maintenance-mode wrap when `spec.pgdog.replicas > 1`), recording the applied hash in status. Watches `GresTenant`s (`Controller::watches`) so tenant changes requeue the fleet. Steps: mock tests first (render-into-Secret sequence, RELOAD-on-hash-change against a faked admin-pg seam, no-op when hash unchanged), implement, nextest/clippy/fmt, commit `feat(operator): Gres fleet reconciler with PgDog aggregation and reload`.

---

## Batch 4 — CLI + e2e (run Tasks 9 and 10 in parallel; disjoint files)

### Task 9: `crabka gres` CLI

**Files:** Modify `crates/cli/src/main.rs` (+ new module `src/gres.rs`), `Cargo.toml` (add `crabka-gres-control`, `crabka-security`).

Subcommands: `create-tenant --bootstrap --name --user --password[-file] [--wal-replication] [--bucket-prefix] [--idle-seconds]` (hashes to a verifier client-side; never sends the password anywhere), `describe`, `list`, `suspend`, `resume`, `delete`, `render-pgdog --out-dir [--activator host:port]`. Steps: failing integration test (in-process broker: create → list → describe shows the record sans password; render-pgdog emits files matching the Task-3 goldens for the same input), implement thin clap→library shims (the house "thin clap → lib" bin idiom), nextest/clippy/fmt, `crates/cli/README.md` section, commit `feat(cli): crabka gres tenant management`.

### Task 10: The e2e gate in CI

**Files:** Modify `.github/workflows/ci.yml` (new job `gres-e2e`, `changes` filter additions for `crates/gres-control/**` + operator gres files, `gatekeeper-ci` needs), a driver script `scripts/gres-e2e.sh`.

The script (full content written at execution; shape): start broker (the G-2 CI leg's format+run steps) → `crabka gres create-tenant` ×2 (users `alice`/`bob`) → start two computes (substrate mode) → `crabka gres render-pgdog --activator` omitted → run the **pinned** PgDog container with the rendered config (`docker run -v config:/etc/pgdog ghcr.io/pgdogdev/pgdog:<pin>`) → psql through PgDog to both tenants with each tenant's SCRAM credentials over TLS (self-signed cert from the pgwire fixtures for the client leg) asserting per-tenant data isolation → wrong-password and wrong-tenant-credential attempts fail → kill tenant A's compute, assert tenant B unaffected. All waits bounded condition loops. Steps: script + job (model on `gres-conformance`'s shape; no coverage upload), yaml validate, commit `ci: gres front-door e2e with real PgDog`.

## Completion checklist (maps to the G-4 gate)

- N tenants through one PgDog endpoint with per-tenant SCRAM isolation — the e2e leg (Task 10).
- Registry read-your-writes + fold semantics pinned (Task 1); verifiers round-trip real PostgreSQL material (Task 2).
- Operator reconcile sequences pinned on the mock harness (Tasks 7–8); CRDs regen-checked (Task 6).
- CLI parity with the operator path, including config rendering for non-k8s deploys (Task 9).
