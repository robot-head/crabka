# G-4 front-door closure evidence — 2026-07-11

Scope is G-4 only. This evidence does not claim G-5 or later chapters.

## Pin and renderer compatibility

- Official image: `ghcr.io/pgdogdev/pgdog:0.1.47`
- OCI digest: `sha256:54cafbda4fc8602ae81188db46ad5695038468943c992ad6b0e1de06297e8a66`
- OCI/source revision: `f6eea5e7c7c06f62a72e669c3f3f607f4945658b`
- PgDog reports `v0.1.47 [main@f6eea5e]`.
- The pin was upgraded from 0.1.6 because that release has no
  `tls_client_required` setting and therefore cannot satisfy the G-4 plaintext
  rejection contract. The renderer now uses the pinned fields `port`,
  `tls_certificate`, `tls_private_key`, `tls_client_required`, and millisecond
  timeout units.
- The official image's `configcheck` loaded the CLI-rendered `pgdog.toml` and
  password-free `users.toml` and reported `config valid`. The gate also rejects
  error text because this PgDog command can report an invalid field while
  exiting zero.
- The CLI has byte-exact assertions for both rendered files, so the
  broker-driven render path is pinned to the shared renderer output rather than
  merely checked for selected substrings.

## Complete live front-door gate

Command:

```text
CRABKA_GRES_SKIP_BUILD=1 CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 timeout 1200s ./scripts/gres-e2e.sh
```

Result: PASS, `Gres front-door e2e completed`.

The run proved:

- broker-backed production Registry/CLI create, list, describe, render,
  delete/tombstone; no plaintext password in CLI output or argv;
- three registry-derived compute SCRAM verifiers behind one PgDog endpoint;
- PgDog log entries identify successful clients as `auth: passthrough`, while
  backend connections identify `auth=scram`;
- client TLS 1.3 negotiated with CA and hostname verification;
- plaintext, incorrect CA, wrong password, and wrong-tenant credentials fail;
- exact native Kafka metadata probes return code 29 for tenant A reading tenant
  B's `__gres_cfg.tenant-b`, `__gres_wal.tenant-b.r0`, and global
  `__gres_tenants`; successful/empty/timeout outcomes cannot satisfy the gate;
- tenant data isolation and tenant-B continuity after tenant-A compute death;
- real admin `RELOAD` after mutating tenant C's configured host, followed by
  database/host/port confirmation through PgDog 0.1.47's supported `SHOW POOLS`
  view. `SHOW DATABASES`, named by the draft plan, does not exist in the pinned
  upstream release;
- standard SQL corpus through transaction-mode PgDog: 665/688, matching the
  checked-in pooler baseline; extended protocol: 6/6; concurrent/failure-safe
  extended lifecycle: 3/3;
- Rust tokio-postgres and SQLx, Python psycopg, and the two-logical-client F-1
  GUC transaction-pooler gate.

The one pooler-only corpus deviation from the direct 666/688 baseline is
documented in `crates/gres-conformance/pooler-baseline.md`: PgDog accepts the
deliberately invalid `SET TIME ZONE 'Mars/Phobos'` as logical state. The SQL is
still executed and reported; reconnect-per-file prevents that deliberate error
from poisoning unrelated files.

## Static and generated gates

- `./tools/regen-crds.sh` followed by `git diff --exit-code -- deploy/crds`:
  PASS, zero diff. `.github/workflows/codegen-check.yml` already enforces the
  same regeneration anti-rot check.
- `bash scripts/tests/gres-e2e-topic-probe.sh`: PASS.
- `python3 scripts/tests/gres_f0_runtime_gates.py`: PASS.
- `./tools/check-pg-compat-matrix.sh --self-test` and the normal check: PASS.
- `cargo +nightly fmt --all -- --check`: PASS.
- G-4 focused control, CLI, security, operator, conformance, and Gres unit/test
  targets pass, except two independent later-chapter current-tree failures
  recorded below.
- PgDog/Postgres image pulls and container/process cleanup are bounded in both
  the primary and cold-start gates. Multi-replica operator reloads enumerate
  every PgDog pod, enter maintenance on the complete fleet, compare full route
  tuples exactly, and attempt maintenance rollback/cleanup on every failure
  path. Service DNS remains the TLS identity while per-pod IPs are only TCP
  destinations. A fake admin-command seam proves ON/RELOAD/SHOW/OFF failures,
  all-replica rollback, and dual-error preservation. SHOW POOLS rows decode
  fail-closed; malformed rows cannot disappear into an empty observed set.

Current-tree failures outside this G-4 diff were reported to the coordinating
agent rather than hidden: the G-8 runtime test
`live_multirange_transfer_stages_populated_successor_without_publishing_it`
forces a checkpoint while its WAL writer remains paused, and G-3's non-failpoint
`checkpoint_fail` stub triggers Clippy `unnecessary_wraps`. The coordinating
goal must resolve both before using this document as the final workspace-wide
green claim.

## Consistency amendment

Equal-version registry retries are accepted only when byte-identical. Divergent
snapshots with the same tenant/version are rejected, deliberately replacing the
draft's ordering-dependent last-record-wins tie rule. The policy and rationale
are documented in `crates/gres-control/README.md` and pinned by unit tests.
