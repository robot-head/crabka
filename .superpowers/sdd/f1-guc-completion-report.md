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

## Landed-core review correction pass (2026-07-11)

Status remains **INCOMPLETE** because exact capture-backed driver goldens,
compatibility anti-rot, and M0 publication are outside this correction pass.

### Additional PostgreSQL 18 oracle evidence

The provisioned `crabka-pg18-control` (`postgres:18`) returned:

- `SET DateStyle TO 'ISO, DMY'` -> `ISO, DMY`;
- `SET DateStyle TO 'SQL, European'` -> `SQL, DMY`;
- `SET IntervalStyle TO 'POSTGRES_VERBOSE'` -> `postgres_verbose`;
- invalid `IntervalStyle` -> `22023`-class invalid-value error;
- `SET statement_timeout TO '2s'` -> `2s`;
- `SET statement_timeout TO '1.5s'` -> `1500ms`;
- `SET statement_timeout TO '1 min'` -> `1min`;
- negative timeout -> invalid/out-of-range.

Commands used `docker exec crabka-pg18-control psql -U bob -d tenant-b
-Atqc ...`. The parser acceptance matrix was added to the optional
`libpg_query_oracle` target. Its host build was attempted and stopped before the
tests because `pg_query` bindgen could not find system `stddef.h`; the ordinary
parser target and direct PostgreSQL 18 grammar/value probes remained green.

### RED/GREEN evidence

- RED: `parses_f1_guc_command_surface` rejected `SET SESSION ...` at position
  12. GREEN: SESSION scope, signed integer GUC input, comma DateStyle values,
  SHOW/RESET ALL, DISCARD ALL, and bounded unsupported DISCARD variants parse as
  specified.
- RED: `source_values_drive_default_reset_all_discard_and_pg_settings` observed
  boot `""` after `SET ... DEFAULT`, not configured source
  `configured-source`. GREEN: SET/SET LOCAL DEFAULT, RESET ALL, set_config then
  reset, DISCARD, and `pg_settings.reset_val` all use the independent source;
  `boot_val` remains independent.
- RED: `typed_guc_parsers_match_postgres_18_canonical_forms` rendered
  `SQL, European`. GREEN: the cohesive definition parser produces typed
  DateStyle/IntervalStyle states and PostgreSQL canonical forms/timeout units.
- GREEN: rejected in-transaction DISCARD remains atomic: SQLSTATE `25001`, role,
  GUC, prepared statement, and portal survive; successful DISCARD resets role
  and source-backed GUCs, clears resources, and leaves the connection usable.
- LIVE RED: a one-backend PgDog 0.1.6 pool exposed its documented/pinned session
  limitations (transactional SET leakage and raw timeout rendering). GREEN:
  the final gate uses tracked startup session state, exercises SET, SET LOCAL,
  SHOW/current_setting, rollback, RESET, two concurrently open logical clients,
  and replay on exactly one tenant-b backend. Deviations are checked and
  explained in `crates/gres-conformance/pooler-baseline.md`.

### Implementation corrections

- Every GUC definition now owns its parser/canonicalizer in the registry; the
  second parameter-name validation match was removed. DateStyle and
  IntervalStyle use bounded enums; bool, integer, duration, and text values stay
  typed internally. Construction, SET, and set_config share the same parser.
- Per-slot source values survive DISCARD. Runtime `pg_settings` rows carry
  effective, boot, and reset values independently.
- PgDog configuration uses the general default pool of 10 so concurrent tenant-c
  lifecycle coverage remains valid, while tenant-b is explicitly overridden to
  `pool_size = 1` for the state-reuse proof.

### Verification after corrections

- `cargo test -p crabka-pgparser -p crabka-pgexec -p crabka-pgwire
  -p crabka-gres-conformance --all-targets`: PASS.
- `cargo test -p crabka-pgexec --lib session::tests:: -- --nocapture`: 44/44
  PASS.
- `cargo clippy -p crabka-pgparser -p crabka-pgexec -p crabka-pgwire
  -p crabka-gres-conformance --all-targets -- -D warnings`: PASS.
- `cargo +nightly fmt --all -- --check`, `git diff --check`, `bash -n
  scripts/gres-e2e.sh`, and `python3 scripts/tests/gres_f0_runtime_gates.py`:
  PASS.
- `PATH=/tmp/crabka-f1-venv/bin:$PATH CRABKA_GRES_SKIP_BUILD=1
  CRABKA_GRES_EXPECT_KAFKA_ACL=0 CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 timeout 300s
  bash scripts/gres-e2e.sh`: PASS for the full PgDog/front-door flow, including
  lifecycle 3/3, parity 666/688 at baseline, extended parity 6/6, Rust driver,
  psycopg 3.2.9, and the F-1 one-backend/two-client GUC gate. Kafka ACL was
  explicitly skipped in this rerun because it was not changed by this pass.
