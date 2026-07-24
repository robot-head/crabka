# PgDog Front-door Runtime Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task by
> task.

**Goal:** Expose every PgDog pool/runtime choice and Gres-controller timing
policy through validated CRD or CLI/environment configuration while retaining
only compatibility, topology, and safety invariants as fixed values.

**Architecture:** `crabka-gres-control` continues to render one typed
`PgdogGeneral`/`PgdogTimeouts` input, but accepts an explicit validated attempt
count and correctly falls back from tenant to fleet pooler mode. Operator-owned
fleet settings live under `Gres.spec.pgdog`; operator-process retry/requeue
settings live in `OperatorConfig` with CLI/environment backing. The standalone
`crabka gres render-pgdog` command exposes the same rendering inputs through
CLI/environment flags. Derived timeout arithmetic remains checked and has one
source of truth.

**Tech Stack:** Rust 2024, `refined_type` 0.6, Clap, kube/schemars CRDs,
Cargo nextest.

## Constraints

- Preserve current operator defaults and output:
  - pooler mode `transaction`;
  - connect attempts `3`;
  - activator attempt timeout `30000ms`, yielding a checked `90000ms`
    cold-start ceiling, `30000ms` connect timeout, and `90000ms` checkout;
  - normal idle timeout `60000ms`;
  - suspension idle timeout `1000ms`;
  - server lifetime `300000ms`;
  - PgDog readiness period `5s`;
  - direct-bootstrap credential grace `4000ms`;
  - reload attempts `3`, backoff `100ms`, stale requeue `15000ms`, admin
    timeout `20000ms`, transition fallback `60000ms`, controller error requeue
    `15000ms`.
- Preserve standalone `render-pgdog` defaults/output: cold-start ceiling
  `30000ms`, attempts `3`, derived connect timeout `10000ms`, checkout
  `30000ms`.
- New validated newtypes use `refined_type`; never use
  `Refined::unsafe_new`.
- Every standalone/direct value has an environment-backed Clap option. CLI
  wins over environment.
- Validate the full PgDog/activator coupling before any child API write.
- Do not configure derived connect/checkout values independently; derive them
  from configured ceiling/attempt timeout plus attempt count.
- Use `assert2::assert!`; add no lint suppressions or new third-party
  dependencies.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

## Fixed Values

Keep these fixed because changing them would violate scale-to-zero,
compatibility, topology, or safety:

- `min_pool_size = 0`;
- PgDog's disabled idle-healthcheck sentinel `3155760000000ms`;
- passthrough authentication fixed enabled for the forwarding model, and
  TLS-required state derived from mounted frontend TLS;
- internal activator/compute ports, TCP, command/config paths, UID/GID,
  labels, names, and admin command/column protocol;
- the one-millisecond minimum scheduled delay, saturating Unix-deadline
  arithmetic, and checked duration conversions.

---

### Task 1: Make PgDog Rendering Policy Explicit

**Files:**

- Modify: `crates/gres-control/src/pgdog.rs`
- Modify: `crates/gres-control/src/lib.rs`
- Modify: PgDog golden/unit tests

- [x] **Step 1: Add failing boundary and rendering tests**

Cover:

- positive attempt count accepts `1` and `65535`, rejects `0`;
- checked `attempt_timeout × attempts` exact result and overflow rejection;
- `for_cold_start_ceiling` honors configured attempts;
- explicit timeouts still must cover the cold-start ceiling;
- tenant-specific pooler mode wins; absent tenant mode falls back to the
  fleet-wide mode, not enum default;
- exact current default and custom TOML.

- [x] **Step 2: Add the minimum refined attempt type**

Export one small `FromStr`-capable positive `u16` wrapper backed by
`refined_type`. Reuse the existing refined positive-millisecond wrapper where
possible; do not add a policy framework.

Change the checked APIs to accept explicit attempts:

```rust
PgdogTimeouts::cold_start_ceiling_for_attempt_timeout(attempt_timeout, attempts)
PgdogTimeouts::for_cold_start_ceiling(cold_start_ceiling, attempts)
```

Store/render the validated count. Remove `DEFAULT_CONNECT_ATTEMPTS` from
arithmetic ownership; defaults belong at input surfaces.

- [x] **Step 3: Fix fleet pooler fallback**

Pass `input.general.pooler_mode` into database rendering. Use
`tenant.pooler_mode.unwrap_or(input.general.pooler_mode)`. Preserve explicit
tenant overrides.

- [x] **Step 4: Verify Task 1**

Run full `gres-control` tests, strict all-target Clippy, nightly formatting,
golden checks, and `git diff --check`.

---

### Task 2: Expose Standalone `render-pgdog` Inputs

**Files:**

- Modify: `crates/cli/src/gres.rs`
- Modify: focused CLI tests/goldens

- [x] **Step 1: Add failing parsing/precedence tests**

Cover exact defaults, environment-only values, CLI-over-environment
precedence, zero/overflow rejection, and exact rendered TOML for custom values.

- [x] **Step 2: Add environment-backed render options**

Expose:

| CLI | Environment | Default |
|---|---|---:|
| `--bootstrap` | `CRABKA_GRES_PGDOG_BOOTSTRAP` | required |
| `--out-dir` | `CRABKA_GRES_PGDOG_OUT_DIR` | required |
| `--activator` | `CRABKA_GRES_PGDOG_ACTIVATOR` | absent |
| `--listen-port` | `CRABKA_GRES_PGDOG_LISTEN_PORT` | 6432 |
| `--tls-certificate` | `CRABKA_GRES_PGDOG_TLS_CERTIFICATE` | absent |
| `--tls-private-key` | `CRABKA_GRES_PGDOG_TLS_PRIVATE_KEY` | absent |
| `--tls-client-ca-certificate` | `CRABKA_GRES_PGDOG_TLS_CLIENT_CA_CERTIFICATE` | absent |
| `--pooler-mode` | `CRABKA_GRES_PGDOG_POOLER_MODE` | transaction |
| `--connect-attempts` | `CRABKA_GRES_PGDOG_CONNECT_ATTEMPTS` | 3 |
| `--cold-start-ceiling-ms` | `CRABKA_GRES_PGDOG_COLD_START_CEILING_MS` | 30000 |
| `--idle-timeout-ms` | `CRABKA_GRES_PGDOG_IDLE_TIMEOUT_MS` | 60000 |
| `--suspension-idle-timeout-ms` | `CRABKA_GRES_PGDOG_SUSPENSION_IDLE_TIMEOUT_MS` | 1000 |
| `--server-lifetime-ms` | `CRABKA_GRES_PGDOG_SERVER_LIFETIME_MS` | 300000 |

Use the suspension idle timeout only when at least one rendered tenant has
`idle_seconds > 0`; `Some(0)` means never suspend and must not select it.
Build `PgdogGeneral` explicitly from these values. Require the TLS certificate
and private key together, and allow the client CA only when that pair is
present.

- [x] **Step 3: Verify Task 2**

Run full CLI tests, strict all-target/all-feature Clippy,
`crabka gres render-pgdog --help`, nightly formatting, and diff checks.

---

### Task 3: Add Typed `Gres.spec.pgdog` Runtime Policy

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/crd/mod.rs`
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify: focused CRD/reconcile tests
- Regenerate: `deploy/crds/crabka.io_greses.yaml`

- [x] **Step 1: Add failing CRD/render/coupling tests**

Cover:

- JSON/YAML roundtrip and OpenAPI bounds/enums;
- exact absent-field defaults;
- invalid/zero/overflow failure before Kube child writes;
- invalid listen port no longer silently falls back to 6432;
- default/custom PgDog TOML and readiness period;
- fleet pooler mode reaches global and database entries;
- connect attempts drive checked activator timeout ceiling;
- suspension timeout is selected only for matching tenants whose effective
  idle setting is greater than zero;
- `idleSeconds: 0` and unrelated fleets do not select suspension timeout;
- the same direct-bootstrap grace drives tenant status creation, credential
  retention, route expiry, and transition scheduling.

- [x] **Step 2: Extend `PgdogSpec`**

Add optional fields:

- `poolerMode`;
- `connectAttempts` (`1..=65535`);
- `idleTimeoutMs` (positive);
- `suspensionIdleTimeoutMs` (positive);
- `serverLifetimeMs` (positive);
- `readinessProbePeriodSeconds` (positive);
- `directBootstrapGraceMs` (positive).

Use a schema enum for pooler mode and refined-backed conversion for positive
values. Validate `listen_port` through the same fail-fast path; remove
`unwrap_or(6432)`.

- [x] **Step 3: Thread one effective policy**

Use configured attempts when multiplying the activator attempt timeout and
when deriving PgDog timeouts. Render configured pooler/idle/lifetime values.
Inspect only tenants belonging to this Gres fleet and their effective
idle/default/override values when selecting suspension idle timeout.

Pass direct-bootstrap grace into GresTenant status updates instead of keeping
the duplicate `4000ms` constant. Use the same value in Gres credential
retention and `next_pgdog_transition_requeue`.

- [x] **Step 4: Generate and verify**

Run Gres-control/operator focused and full suites, strict all-target Clippy,
nightly formatting, exact all-CRD generation, and diff checks.

---

### Task 4: Expose Operator Controller Timing

**Files:**

- Modify: `crates/operator/src/config.rs`
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: controller `error_policy` functions currently fixed at 15 seconds
- Modify: shared operator test configuration/fixtures

- [x] **Step 1: Add failing defaults/env/precedence tests**

Add exact tests for:

- `PGDOG_RELOAD_ATTEMPTS` / `--pgdog-reload-attempts` = 3;
- `PGDOG_RELOAD_BACKOFF_MS` / `--pgdog-reload-backoff-ms` = 100;
- `PGDOG_RELOAD_REQUEUE_MS` / `--pgdog-reload-requeue-ms` = 15000;
- `PGDOG_ADMIN_TIMEOUT_MS` / `--pgdog-admin-timeout-ms` = 20000;
- `PGDOG_TRANSITION_POLL_MS` / `--pgdog-transition-poll-ms` = 60000;
- `CONTROLLER_ERROR_REQUEUE_MS` / `--controller-error-requeue-ms` = 15000.

Cover environment-only, CLI precedence, and zero rejection.

- [x] **Step 2: Route Gres controller timing**

Replace fixed reload attempt/backoff, reload requeue, admin timeout, and
transition fallback values with `OperatorConfig`. Keep grace-boundary-derived
requeues and the one-millisecond minimum derived.

- [x] **Step 3: Route common error requeue**

Use `ctx.config.controller_error_requeue_ms` in every controller error policy
currently hardcoded to 15 seconds: Kafka, KafkaNodePool, KafkaTopic,
KafkaUser, SchemaRegistry, KafkaGrpcGateway, Gres, and GresTenant. Leave
KafkaRebalance's distinct `TRANSPORT_RETRY` for its own semantic slice.

- [x] **Step 4: Verify Task 4**

Run operator config/controller tests, full operator nextest, strict all-target
Clippy, operator help/env evidence, nightly formatting, and diff checks.

---

### Task 5: Audit and Close the PgDog Front-door Sub-slice

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify: this plan
- Modify code/tests only for genuine audit misses

- [x] **Step 1: Run scanner and semantic audit**

Run `tools/audit-runtime-values.sh`. Inspect every PgDog renderer input,
operator Gres timing literal, CRD field, CLI/environment binding, and derived
timeout/grace call path.

- [x] **Step 2: Prove fixed/configurable boundaries**

Verify all configurable values above are explicit and that the Fixed Values
list remains fixed for stated compatibility/safety reasons. Prove no silent
clamps/fallbacks, zero acceptance, ineffective pooler mode, duplicated grace,
or cross-fleet idle selection remains.

- [x] **Step 3: Run closure gates**

Run affected full nextest suites, strict all-target/all-feature Clippy, CLI and
operator help surfaces, nightly formatting, exact all-CRD generation, scanner,
focused semantic `rg` checks, and `git diff --check`.

- [x] **Step 4: Document evidence and remaining Gres work**

Record exact scanner counts/classifications and gate results. Keep other
Gres/GresTenant/compute/loadtest policy Pending unless independently audited;
do not claim the whole Gres family complete.

Task 5 closed the PgDog/front-door sub-slice on 2026-07-24 without code
remediation. The scanner reported 5,902 repository matches: 41 across three
`gres-control` files, 27 across two `cli` files, and 171 across 18 `operator`
files. The focused paths contributed 26, 20, 17, 13, and one match in
`gres-control/src/pgdog.rs`, `cli/src/gres.rs`,
`operator/src/controller/gres.rs`,
`operator/src/controller/gres_tenant.rs`, and `operator/src/config.rs`,
respectively.

The semantic audit proved all 13 standalone CLI/environment pairs, seven
optional PgDog CRD fields, six operator timing pairs, one attempt-count and
direct-bootstrap-grace source each, matching-fleet idle/deadline selection,
eight common error-policy users, bounded transition polling, and the unchanged
KafkaRebalance transport retry. The full three-package nextest run passed
1,058 tests with none skipped; strict all-target/all-feature Clippy, both help
surfaces, nightly formatting, exact regeneration of all nine CRDs, focused
`rg`, scanner, and `git diff --check` passed. Exact classifications and
commands are recorded in `docs/configuration-audit.md`; broader Gres policy
remains Pending.
