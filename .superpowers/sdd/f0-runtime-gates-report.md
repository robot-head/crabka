# F-0 runtime gates implementation report

## Status

DONE_WITH_CONCERNS. The deterministic and crate-local gates pass. The live
Docker E2E was attempted but did not reach PgDog because the repository-wide
build failed in `crabka-gres-ranges` on unrelated `Session` trait drift. This
host also does not have Python `psycopg` installed; the E2E now reports that
prerequisite explicitly, and CI installs pinned, hash-verified packages.

Implementation commit: `0dbfe944` (`test(gres): enforce F-0 runtime gates`).

## RED evidence

Command:

```sh
bash scripts/tests/gres-f0-runtime-gates.sh
```

Observed result: exit status 1 at the first absent PgDog extended-corpus
invocation. At that point the script also specified the missing standalone and
substrate artifact names, Python-driver installation, Rust driver binary, sqlx
dependency, and three-driver E2E wiring.

## Implementation

- Added separately named standalone and substrate extended-corpus CI legs,
  summaries, and uploaded JSON/Markdown reports using the checked-in 6/6
  baseline.
- Added the extended corpus and separately named reports to the transaction-mode
  PgDog E2E artifact directory; CI preserves and uploads the directory on every
  non-cancelled run.
- Added `crabka-gres-driver-smoke`, which executes and asserts `$1`-parameterized
  queries via both `tokio-postgres` and workspace `sqlx` across two independent
  transactions per driver.
- Added a real Python `psycopg` path with two parameterized transactions and
  asserted results. Missing psycopg is a clear hard failure unless the explicit
  pre-existing `--skip-pgdog` development path is used.
- Added pinned, SHA-256-verified Python requirements and documented exact local
  commands and artifact behavior.
- Did not modify parity baselines, corpus cases, pooler mode, or image pins.

## GREEN evidence

```text
bash scripts/tests/gres-f0-runtime-gates.sh
PASS: F-0 runtime gate wiring contract

cargo nextest run -p crabka-gres-conformance --no-fail-fast
Summary: 20 tests run: 20 passed, 0 skipped

cargo check -p crabka-gres-conformance --all-targets
Finished dev profile successfully

cargo clippy -p crabka-gres-conformance --all-targets -- -D warnings
Finished dev profile successfully

cargo +nightly fmt --all -- --check
PASS (no diff)

git diff --check
PASS (no output)

bash -n scripts/gres-e2e.sh scripts/tests/gres-f0-runtime-gates.sh
PASS (no output)
```

## Live E2E attempt

Docker was available (`docker info` exited 0), so the following was attempted:

```sh
CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 ./scripts/gres-e2e.sh
```

The required workspace build failed before runtime setup with these existing
cross-crate API errors in `crates/gres-ranges/src/tenant.rs`:

```text
method `extended_query` is not a member of trait `Session`
method `describe` is not a member of trait `Session`
not all trait items implemented, missing: `parse`, `bind`,
`describe_statement`, `describe_portal`, `execute`, `close`, `sync`
```

Therefore the live PgDog gate is not claimed as passed. The local Python also
reported `ModuleNotFoundError: No module named 'psycopg'`; CI owns installation
through `requirements-driver-smoke.txt` before invoking the mandatory live gate.

## Self-review

Connection credentials are passed to the Rust and Python drivers through the
environment and are not printed on driver failures. Every driver reuses one
client connection across two explicit, committed transactions. PgDog remains
in its rendered transaction-pooling mode, waits remain bounded, and the
explicit skip path returns before requiring Docker or psycopg.

## Concerns

- Live evidence remains pending until the unrelated `crabka-gres-ranges`
  `Session` implementation compiles in the integrated workspace.
- The pinned pure-Python psycopg package relies on libpq supplied by the pinned
  PostgreSQL 18 client installation in CI.
