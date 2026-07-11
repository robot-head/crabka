# Extended corpus transaction-scoped execution report

## Status

Fixed the two F-0 extended corpus cases by running every case inside an explicit
`tokio_postgres::Transaction`, always rolling it back, and using only supported
F-0 table syntax. The focused live PgDog regression and the unchanged 6/6
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

The focused live test also caught that rollback alone does not currently remove
Gres DDL. Plain supported `DROP TABLE` teardown is therefore executed before the
final rollback. The test proves the prepared query succeeds through PgDog and a
post-case lookup returns `42P01`.

## Implementation

- `run_extended_one` borrows a mutable client, opens a
  `tokio_postgres::Transaction`, runs setup/query/teardown on that transaction,
  and always attempts rollback.
- Setup, query, and teardown errors remain primary. A rollback error is visible
  only when the case otherwise succeeded.
- The two case-unique corpus tables use ordinary `CREATE TABLE` and plain
  `DROP TABLE`; no shared global names, session pooling, or baseline weakening
  was introduced.
- The complete E2E runs the focused real PgDog regression before conformance.
- Mandatory driver smokes now target surviving tenant B because the script
  deliberately kills tenant A before the smoke phase.

## Verification

- `cargo test -p crabka-gres-conformance`: passed (22 unit tests, 1 integration
  test skipped cleanly without its live URL, doc tests passed).
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

- Focused transaction-scoped PgDog regression: passed, including post-cleanup
  `42P01`.
- Standard parity: 666/688, baseline floor passed.
- Extended parity: 6/6, 100%, baseline floor passed.
- tokio-postgres smoke on surviving tenant B completed before the sqlx
  connection failed.
- sqlx smoke: blocked by PgDog `ProtocolOutOfSync got: C`, surfaced as SQLSTATE
  `58000` (`protocol is out of sync`).
- psycopg smoke and the final E2E PASS line were not reached.

Artifacts are retained under `target/gres-e2e-artifacts/`.

## Self-review

The corpus change is limited to replacing unsupported D7 setup syntax with
supported transaction-scoped F-0 setup and deterministic teardown. All case
names, SQL under test, parameters, normalized comparison behavior, and the 6/6
baseline remain unchanged. Cleanup is attempted on every case path, and cleanup
failures cannot replace an earlier setup/query error.
