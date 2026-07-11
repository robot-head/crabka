# F-1 typed GUC completion report

Date: 2026-07-11

## Status

INCOMPLETE pending independent clean re-review. The typed registry/transaction
stack, parser surface, DISCARD core, hardened capture-backed exact-driver/PgDog
goldens, executable direct-Gres replay, compatibility anti-rot, mandatory live
PgDog E2E, and dated M0 evidence are implemented and green. M0 is not marked
complete until review confirms the remediation below; it covers F-0/F-1 only
and does not claim later SQL waves or full PostgreSQL parity.

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
- `e81d7c5d` `fix(gres): close F-1 session core review gaps`

## Static and local verification

- `cargo test -p crabka-pgparser -p crabka-pgexec -p crabka-pgwire -p crabka-gres-conformance --all-targets`: PASS.
- `cargo clippy -p crabka-pgexec --all-targets -- -D warnings`: PASS.
- `python3 scripts/tests/gres_f0_runtime_gates.py`: PASS.
- `bash -n scripts/gres-e2e.sh`: PASS.
- `git diff --check`: PASS.
- Complete correction-pass verification and live results are recorded below.

## Resolved exit blockers

- Exact payload-decoded, allowlisted startup captures and corresponding PgDog
  backend SET captures are checked in for tokio-postgres 0.7.18, SQLx 0.9.0,
  and psycopg 3.2.9. Empty batches are explicit rather than inferred.
- Compatibility anti-rot and dated M0 matrix publication are green and linked
  from `docs/PG_COMPAT_MATRIX.md`.

## Landed-core review correction pass (2026-07-11)

Historical status at the end of this correction pass was **INCOMPLETE** because
exact capture-backed driver goldens, compatibility anti-rot, and M0 publication
were outside that pass. The exit wave below resolves those items.

### Additional PostgreSQL 18 oracle evidence

The provisioned `crabka-pg18-control` (`postgres:18`) returned:

- `SET DateStyle TO 'ISO, DMY'` -> `ISO, DMY`;
- `SET DateStyle TO 'SQL, European'` -> `SQL, DMY`;
- `SET IntervalStyle TO 'POSTGRES_VERBOSE'` -> `postgres_verbose`;
- invalid `IntervalStyle` -> `22023`-class invalid-value error;
- `SET statement_timeout TO '2s'` -> `2s`;
- `SET statement_timeout TO '1.5s'` -> `1500ms`;
- `SET statement_timeout TO '1 min'` -> `1min`;
- `SET statement_timeout TO '.5s'` -> `500ms`;
- `SET statement_timeout TO '1h'` -> `1h`;
- `SET statement_timeout TO '1d'` -> `1d`;
- `SET DateStyle TO 'SQL, DMY'; SET DateStyle TO 'MDY'` -> `SQL, MDY`;
- `BEGIN; SELECT 1; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` -> error
  `SET TRANSACTION ISOLATION LEVEL must be called before any query` (25001
  class);
- the same late SET TRANSACTION error follows successful INSERT, UPDATE, DELETE,
  and CREATE TEMP TABLE; SHOW, SET GUC, RESET GUC, and repeated transaction
  controls do not establish activity and leave SET TRANSACTION allowed;
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
  the final gate directly observes a distinct SET on the assigned backend,
  restores the tracked startup value before release, and then exercises SET
  LOCAL, SHOW/current_setting, rollback, RESET, two concurrently open logical
  clients, and startup-state replay on exactly one tenant-b backend. It does not
  claim SET-change replay. Deviations are checked and explained in
  `crates/gres-conformance/pooler-baseline.md`.
- RED: partial DateStyle assignment reset unspecified components to boot values;
  unquoted `SQL DMY` left trailing parser input; `1h` rendered `60min`; and SET
  TRANSACTION remained legal after SELECT. GREEN: partial assignments inherit
  current typed components, bounded multi-token parsing stops at statement
  boundaries, `.5s`/hours/days render like PostgreSQL 18, and post-query SET
  TRANSACTION fails with SQLSTATE 25001 and aborts the block.
- RED: INSERT/UPDATE/DELETE and supported CREATE TABLE left SET TRANSACTION legal.
  GREEN: an exhaustive statement classifier marks successful query, DML, and DDL
  variants as transaction activity while preserving PostgreSQL 18's non-marking
  SHOW/SET/RESET/transaction-control behavior; late SET returns 25001 and fails
  the block for every covered activity class.

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
- `cargo test -p crabka-pgexec --lib session::tests:: -- --nocapture`: PASS.
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

## Capture-backed driver goldens and M0 exit (2026-07-11)

Commit `bbe43a9b` adds the payload-safe recorder, deterministic recapture
orchestrator, schema/secret/version validator, checked fixture, raw-startup
replay, bounded Gres gate, and mandatory CI wiring.

### RED/GREEN and capture evidence

- RED: the validator test failed because the fixture/module did not exist, then
  because the placeholder schema was version 0. GREEN: schema version 2,
  complete provenance, exact dependency pins, strict startup-key allowlist,
  explicit empty arrays, and forbidden identity/secret scanning all pass.
- RED: the first real PgDog backend trace presented an SSLRequest before the
  startup packet. GREEN: a focused recorder test proves the standard `N`
  plaintext negotiation path and the capture completed without retaining
  encrypted or raw bytes.
- Direct PostgreSQL 18 captures: tokio-postgres sends
  `client_encoding=UTF8`; SQLx sends `DateStyle=ISO, MDY`, `TimeZone=UTC`,
  `client_encoding=UTF8`, and `extra_float_digits=2`; psycopg sends no
  non-identity startup settings. All direct simple-query SET arrays are empty.
- PgDog 0.1.6 backend captures: tokio-postgres and psycopg produce empty SET
  arrays; SQLx produces exactly `SET "datestyle" TO 'ISO, MDY'`,
  `SET "extra_float_digits" TO '2'`, and `SET "timezone" TO 'UTC'`, in that
  order. PgDog backend startup was consistently
  `application_name=PgDog, client_encoding=utf-8`.
- The exact recapture command
  `PATH=/tmp/crabka-f1-venv/bin:$PATH python3
  tools/capture-gres-driver-goldens.py --write` provisioned a fresh
  `postgres:18`, verified PgDog image digest and dependency pins, captured all
  six paths, and reproduced the checked fixture.
- RED: the structural gate reported the missing `Captured driver startup
  replay` CI step. GREEN: the mandatory Gres job runs the bounded replay script.
- `CRABKA_GRES_SKIP_BUILD=1 timeout 30s
  ./scripts/gres-driver-goldens-gate.sh`: PASS; three direct and three PgDog
  backend startup packets plus every captured backend SET batch were accepted
  directly by Gres.

### Final static and live exit evidence

- Matrix `--self-test` and normal anti-rot checks: PASS.
- Relevant all-target test/check/clippy (`-D warnings`) commands: PASS.
- Nightly format, diff check, F-0 structural gate, recorder unit tests, and
  direct-Gres replay: PASS.
- Complete provisioned PgDog 0.1.6 E2E: PASS with both pinned Rust drivers,
  psycopg 3.2.9, F-1 one-backend/two-client GUC gate, lifecycle 3/3, base floor
  666/688, and extended parity 6/6.
- The Kafka ACL leg was explicitly disabled for this unchanged rerun and is not
  claimed as executed. The previously reviewed exact-topic/code-29 probe is the
  unchanged ACL evidence.
- Dated M0 evidence is published at
  `docs/superpowers/evidence/2026-07-11-gres-m0.md`. PgDog SET/SHOW/RESET
  limitations remain checked, precisely bounded deviations in
  `crates/gres-conformance/pooler-baseline.md`.

## Independent-review remediation (2026-07-11)

Status: **INCOMPLETE pending clean re-review**. Commit `1ed678a4` addresses all
findings from the first review.

### RED/GREEN

- RED: arbitrary text beginning with SET was retained, SET ROLE/unknown GUCs
  were not rejected, and startup duplicates overwrote earlier values. GREEN:
  the recorder parses only the three actually observed GUC/value assignments,
  deterministically re-renders them, rejects private values, SET ROLE, unknown
  GUCs, mixed statements, comments, semicolon tricks, malformed/duplicate/
  unexpected startup fields, and never serializes arbitrary query text.
- RED: the Rust validator allowed evidence deletion and mutation so long as the
  broad shape remained valid. GREEN: it independently requires the exact three
  drivers in order, exact direct and PgDog-backend startup maps, exact ordered
  SQLx SET list and explicit empty lists, unique JSON startup keys, exact safe
  SQL grammar/values, and exact dependency/image provenance. Deletion,
  reordering, private values, and digest mutation all fail focused tests.
- RED: recorder accept/session time and recapture subprocesses were not all
  absolutely bounded. GREEN: the recorder has one non-renewing absolute
  deadline; every subprocess, image pull, Docker action, and driver has a
  timeout; failure cleanup kills and reaps recorder and containers.
- RED: new tooling paths did not trigger the Gres CI job. GREEN: every capture,
  recorder, test, gate, and structural-test path is asserted in the Gres filter,
  and CI runs the Python safety contract.

### Authoritative capture and provenance

- PgDog is run by exact digest reference
  `ghcr.io/pgdogdev/pgdog@sha256:5d21fa668d091ae6ce30e5cb1536c7bcaba1f96b0d492227b1a46852d1f3ab2c`.
  Inspected OCI labels verify full revision
  `c99282e9001f66194b03b108ba2a66ad7a27a75d`, source
  `https://github.com/pgdogdev/pgdog`, and version `v0.1.6`.
- PostgreSQL is run by exact digest reference
  `postgres@sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4`;
  its inspected image id is pinned and its absent OCI labels are recorded.
- The psycopg wheel SHA-256 is parsed from the one exact pinned requirements
  line and compared to the fixture; it is not embedded as a validator suffix.
- PgDog backend startup parameters are stored per driver and replayed. The gate
  now proves three direct plus three backend startups and all captured SETs.
- Digest-addressed recapture reproduced the fixture byte-for-byte:
  `25251f3ed66931c0f2f4c6fb0e073515b27171abcc97c7f99ca26275b7b69211`.

### Reverification after remediation

- 10 Python payload/timeout/anti-rot tests: PASS.
- 4 Rust schema/provenance/exact-evidence tests: PASS.
- Matrix self-test/normal, relevant all-target test/check/clippy with
  `-D warnings`, nightly fmt, F-0 structural filter/CI contract, six-startup
  replay, and diff check: PASS.
- Full provisioned PgDog 0.1.6 E2E: PASS; lifecycle 3/3, base 666/688,
  extended 6/6, exact Rust/Python drivers, and F-1 two-client GUC gate green.
- ACL was explicitly skipped and is not claimed; the prior reviewed exact-topic
  code-29 evidence remains unchanged.
