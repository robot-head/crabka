# Extended corpus transaction-scoped execution report

## Status

Fixed the two F-0 extended corpus cases with a collision-free, failure-safe case
lifecycle. Every setup and prepared query runs inside one explicit transaction;
successful teardown runs before rollback, while every failed setup, parameter
conversion, prepare, query, or teardown is followed by rollback and independent
committed cleanup. The focused live PgDog lifecycle suite and the unchanged 6/6
extended parity baseline pass.

The complete E2E does not reach its final PASS line: after the semantic gates,
PgDog 0.1.6 reports `ProtocolOutOfSync got: C` during the sqlx smoke and the
driver returns SQLSTATE `58000`. This is retained as a separate downstream
blocker rather than hidden by a retry or workaround.

## Root cause and RED evidence

The retained 4/6 result initially appeared to show a transaction-pooling
affinity failure because both failed cases returned `42P01` after creating
temporary tables. Three hypotheses were tested before correcting that diagnosis:

1. The original autocommit harness failed the new live PgDog regression.
2. A raw `BEGIN`/`ROLLBACK` wrapper still failed.
3. `tokio_postgres::Transaction` with the original setup still failed.

Direct Gres reproduction then isolated the actual setup failures:
`DROP TABLE IF EXISTS missing` returns `42P01`, and `CREATE TEMP TABLE` returns
`42601`. Those features belong to the later D7 SQL-parity sequence and cannot
serve as F-0 setup primitives. An explicit transaction containing ordinary
`CREATE TABLE`, `INSERT`, prepared query execution, and cleanup succeeds.

The first review RED run proved the prior implementation leaked a table after a
setup error following `CREATE TABLE`. A later GREEN run proved cleanup statements
must be isolated: one intentional cleanup failure otherwise suppressed the drop
needed for a clean rerun.

## Implementation

- `run_extended_one` opens a `tokio_postgres::Transaction` for setup and the
  prepared query and always attempts rollback.
- Successful teardown runs in the case transaction. After any earlier failure,
  teardown is retried statement-by-statement in fresh committed transactions so
  one cleanup error cannot suppress later cleanup.
- The first setup/query/parameter/prepare/teardown error remains primary. A
  cleanup error is visible only when no earlier error exists.
- Corpus identifiers contain `__case_id__`, expanded per invocation to an
  ASCII-safe timestamp/process/counter suffix. Concurrent and interrupted runs
  cannot collide, while query and parameter shapes remain unchanged.
- The complete E2E runs the focused real PgDog regression before conformance.
- Mandatory driver smokes now target surviving tenant B because the script
  deliberately kills tenant A before the smoke phase.

## Verification

- `cargo test -p crabka-gres-conformance`: passed (23 unit tests, 3 integration
  tests skipped cleanly without their live URL, doc tests passed).
- `cargo check -p crabka-gres-conformance --all-targets`: passed.
- `cargo clippy -p crabka-gres-conformance --all-targets -- -D warnings`: passed.
- `cargo +nightly fmt --all -- --check`: passed.
- `bash -n scripts/gres-e2e.sh`: passed.
- `git diff --check`: passed.
- Rebuilt skip-build binaries with `cargo build --locked -p crabka-cli
  -p crabka-broker -p crabka-gres -p crabka-gres-conformance`.

## Live evidence

Ran with `target/gres-driver-venv/bin` first in `PATH`,
`CRABKA_GRES_SKIP_BUILD=1`, and retained artifacts.

- Focused real PgDog lifecycle suite: 3/3 passed. It proves rollback-sensitive
  DML, setup/parameter/query failure cleanup and rerun recovery, primary-error
  precedence, cleanup-only errors, and concurrent cases.
- Standard parity: 666/688, baseline floor passed.
- Extended parity: 6/6, 100%, baseline floor passed.
- tokio-postgres smoke on surviving tenant B completed before the sqlx
  connection failed.
- sqlx smoke: blocked by PgDog `ProtocolOutOfSync got: C`, surfaced as SQLSTATE
  `58000` (`protocol is out of sync`).
- psycopg smoke and the final E2E PASS line were not reached.

Artifacts are retained under `target/gres-e2e-artifacts/`.

## Self-review

The corpus change is limited to supported transaction-scoped F-0 setup,
deterministic teardown, and per-invocation identifier expansion. Case names,
query/parameter behavioral shapes, normalized comparison behavior, and the 6/6
baseline remain unchanged. Cleanup is attempted on every case path, continues
past individual cleanup failures, and cannot replace an earlier primary error.
