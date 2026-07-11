# F-1 typed GUC completion report

Date: 2026-07-11

## Status

INCOMPLETE. The typed registry/transaction stack and DISCARD core are implemented,
but the exact capture-backed driver/PgDog goldens and mandatory live E2E evidence
are not complete. Accordingly the M0 matrix was not advanced.

## PostgreSQL 18 oracle

Control: `postgres:18` at the provisioned `crabka-pg18-control` container.

| Ordering | Effective before end | After COMMIT | After ROLLBACK |
| --- | --- | --- | --- |
| SET s1; LOCAL l1; SET s2 | s2 | s2 | entry value |
| LOCAL l1; SET s1; LOCAL l2 | l2 | s1 | entry value |
| SET s1; RESET | source value | source value | entry value |
| LOCAL l1; RESET | source value | source value | entry value |
| set_config(local); set_config(session) | session value | session value | entry value |

The oracle also returned SQLSTATE `25001` for `DISCARD ALL` in an explicit
transaction and renders `statement_timeout = 17` as `17ms`.

## RED/GREEN evidence

- RED: focused GUC state test failed because `with_source_values` did not exist.
- GREEN: typed source values reset independently from boot defaults.
- RED: SQL ordering/DISCARD test returned `0A000`, expected PostgreSQL `25001`.
- GREEN: ordered current/session transaction state and `ActiveSqlTransaction` map
  the oracle behavior.
- GREEN: all 41 `pgexec` session tests passed, including ordering, source reset,
  set_config, timeout rendering, and DISCARD recovery.

## Implementation

- One definition registry now owns canonical names/aliases, vartype, boot default,
  typed value kind, validation, and rendering for the nine practical GUCs.
- Runtime slots carry typed source, committed, current transaction, and commit
  candidate values. Later statements determine the current value; the last
  non-LOCAL mutation determines the committed value.
- RESET/RESET ALL use each slot's source value. A supported constructor proves a
  non-boot source value.
- DISCARD ALL uses SQLSTATE 25001 in a block and, when allowed, resets GUCs and
  role and clears engine-owned prepared statements and portals.
- The live E2E script contains a bounded two-logical-client PgDog GUC gate for
  SET, current_setting/SHOW, LOCAL rollback, RESET, and reuse.

## Commits

- `7306b1a5` `fix(gres): model typed transactional GUC state`
- `a339dd16` `test(gres): gate GUCs through transaction pooler`

## Static and local verification

- `cargo test -p crabka-pgparser -p crabka-pgexec -p crabka-pgwire -p crabka-gres-conformance --all-targets`: PASS.
- `cargo clippy -p crabka-pgexec --all-targets -- -D warnings`: PASS.
- `python3 scripts/tests/gres_f0_runtime_gates.py`: PASS.
- `bash -n scripts/gres-e2e.sh`: PASS.
- `git diff --check`: PASS.
- Workspace all-target check/clippy/fmt command was launched; no complete live
  F-1 E2E result is claimed here.

## Remaining concerns / exit blockers

- No exact payload-decoded, allowlisted startup captures and corresponding PgDog
  backend SET captures are checked in for tokio-postgres 0.7.18, sqlx 0.9.0,
  and psycopg 3.2.9. Draft assumptions were deliberately removed after pinned
  source proved SQLx and tokio both send `client_encoding=UTF8` (SQLx also sends
  `extra_float_digits=2`).
- Parser-oracle coverage was not expanded for every requested grammar spelling.
- The mandatory Docker/PgDog live gate, 6/6 extended parity, and all three driver
  completion were not run to a final PASS after these changes.
- Compatibility anti-rot and dated M0 matrix publication remain outstanding.
