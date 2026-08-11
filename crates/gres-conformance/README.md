# crabka-gres-conformance

Differential conformance harness diffing Crabka Gres against a real PostgreSQL
oracle over the wire.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres — a pure-Rust
Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`; see the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

Runs every statement in `corpus/*.sql` through both a real PostgreSQL (the
oracle) and a Crabka Gres subject via the simple query protocol, diffing rows
and SQLSTATEs into `parity.json`/`parity.md`. `--baseline baseline.json` turns
the report into a CI gate: the statement total is pinned and the match count may
only ratchet up. `baseline.json` records the parity of the vendored engine as
measured against the donor repository at import (crabgresql@93f3d17,
postgres:18 oracle); update it only deliberately — e.g. a corpus change, an
engine improvement, or a documented postgres:18 minor-version drift — never to
absorb a regression.

The live two-range `SHARDED` gate uses `sharded-baseline.json`. Sharded tables
have a deliberately narrower mutation and query surface than ordinary tables,
so their parity floor is ratcheted independently against the same corpus and
PostgreSQL 18 oracle.

The substrate-backed leg — the same corpus replayed against a `crabka-gres`
whose WAL is a Kafka tenant topic — uses `substrate-baseline.json`, and
[`substrate-baseline.md`](substrate-baseline.md) names every statement behind
the difference. That leg runs against its own fresh oracle database, so
`current_database()` can never match while the two ends are connected to
differently named databases; that one statement is now the whole difference,
and it is enumerated there rather than absorbed into `baseline.json`. The same
now applies to the primary leg — see
[`substrate-baseline.md`](substrate-baseline.md) for what has to change in the
harness.

## Baseline ratchet

Corpus growth and `baseline.json` changes must land together in the same
reviewed commit or change. A ratchet is valid only when it includes, or directly
references, a parity report showing the new floor against PostgreSQL 18.

Reviewers should block baseline-only edits and baseline changes that are not
paired with the corpus/parity context that explains the new floor. If a change
adds SQL coverage, updates the measured floor, or responds to PostgreSQL 18
oracle drift, keep the corpus diff, `baseline.json`, and parity evidence in one
reviewable unit.

## Adopted pg_regress corpus

The authoritative compatibility gate is PostgreSQL 18.4's unmodified, pinned
`src/test/regress` suite: all 231 tests from the upstream `parallel_schedule`,
run by upstream `pg_regress`. Install its build prerequisites first:

```sh
sudo apt-get install build-essential bison flex perl pkg-config bzip2 curl ca-certificates
```

The cached PostgreSQL build is configured without ICU, readline, or zlib. Prove
the runner against a fresh PostgreSQL 18.4 temporary instance before measuring
Gres, then run Gres in either or both schedule modes:

```sh
./scripts/gres-pg-regress.sh self-check both
./scripts/gres-pg-regress.sh gres serial
./scripts/gres-pg-regress.sh gres parallel
./scripts/gres-pg-regress.sh gres both
```

Each `gres` command performs both PostgreSQL self-checks first and starts a
fresh in-memory Gres process for each requested mode. Runs retain their command,
logs, diffs, and exit status under `target/pg-regress-runs/`; set
`GRES_PG_REGRESS_ARTIFACT_DIR` to choose a new artifact directory and
`GRES_PG_REGRESS_CACHE_DIR` to relocate the verified source/build cache. See
`./scripts/gres-pg-regress.sh --help` for binary, port, timeout, and job-count
overrides.

The authoritative Gres test process uses a 20 MiB blocking-query memory budget.
The serial process uses one Tokio worker plus explicit
backend-process and initial random seeds so progressive diff fingerprints are
reproducible. Parallel mode uses the runtime's normal worker count. These
controls do not normalize captured output, and production defaults stay
randomized. The runner exposes `GRES_PG_REGRESS_TOKIO_WORKERS`,
`GRES_PG_REGRESS_PROCESS_TOKEN`, and `GRES_PG_REGRESS_RANDOM_SEED` when a
different diagnostic configuration is needed.

Serial runs are checked against
[`pg-regress-baseline.json`](pg-regress-baseline.json). The baseline records the
pinned tag and schedule, every failing test's selected upstream expected file,
changed-line and hunk counts, and a canonical diff fingerprint. CI fails on a
new failure, a changed fingerprint, a larger mismatch, or an unreviewed
improvement. Every run retains `actual-baseline.json` and `summary.md` beside
the raw outputs.

After a semantic fix, update the baseline only from a complete,
infrastructure-clean serial artifact. The helper rejects replacements or
growth and accepts only fewer changed lines or a removed failure:

```sh
RUN=target/pg-regress-runs/<run>/gres-serial
python3 scripts/gres-pg-regress-baseline.py update \
  --postgres-tag REL_18_4 \
  --schedule target/pg-regress-postgresql-18.4/source/src/test/regress/parallel_schedule \
  --tap "$RUN/command.log" --diff "$RUN/regression.diffs" \
  --source-root target/pg-regress-postgresql-18.4/source \
  --build-root "$RUN" \
  --baseline crates/gres-conformance/pg-regress-baseline.json \
  --actual-output "$RUN/actual-baseline.json" \
  --summary-output "$RUN/summary.md"
```

The adopted-corpus score (`9323/14272`, 65.3%) remains useful statement-level
diagnostic evidence, but it is not the compatibility headline. Compatibility
is the unmodified PostgreSQL 18.4 upstream schedule: 231 whole test files in
serial and parallel. The checked-in monotone baseline remains `6/231`; it is
not ratcheted from a non-monotone review.

The latest infrastructure-clean serial certification passes `20/231`, leaving
211 semantic failures and 178550 canonical changed lines across 4574 hunks.
Exact tests are `test_setup`, `md5`, `comments`, `mvcc`, `euc_kr`,
`create_function_c`, `infinite_recurse`, `delete`, `security_label`, `async`,
`dbsize`, `collate.icu.utf8`, `psql_crosstab`, `collate.linux.utf8`,
`collate.windows.win1252`, `vacuum_parallel`, `portals_p2`, `bitmapops`, `numa`,
and `compression_pglz`. Parallel passes `20/231`, leaving 211 semantic failures
and 178708 canonical changed lines across 4575 hunks. Both PostgreSQL
self-checks pass, all 231 Gres tests complete in both modes, and both
infrastructure reports are empty. Certified artifact:
`target/pg-regress-runs/20260803T161026Z-certified-current-gres`. It records observed
conformance without replacing the monotone baseline. Neither result satisfies
the 231/231 completion gate.

This review adds a bounded inclusion-exclusion path for two strict top-level
`OR` branches in scalar `count(*)`, PostgreSQL
`OPERATOR([pg_catalog.]symbol)` expression wrappers, anonymous-record OID 2249
storage and query-field mapping, and native `jsonpath`/`jsonpath[]` type,
catalog, storage, wire, assignment, routine, and domain plumbing. It also
enforces PostgreSQL ALTER pass ordering, expression-index drop dependencies,
and default operator-class gates. Legacy frontend Fastpath messages are fully
consumed and rejected without killing the session; this is protocol
compatibility, not legacy function execution. Native JSONPath plumbing removes
the missing-type failures, but the three upstream JSONPath files remain `0/3`
with 3110 changed lines across 99 hunks because grammar, canonical output, and
evaluator coverage remain incomplete.

This certification also makes `create_function_c`, `delete`, `security_label`,
`dbsize`, `vacuum_parallel`, `numa`, and `compression_pglz` exact. `dbsize`
includes exact `pg_size_bytes` diagnostics, bigint/numeric `pg_size_pretty`, and
physical local-secondary-index key/value sizing. Heap, TOAST, PostgreSQL page
and auxiliary-fork storage, and database/tablespace totals remain zero;
cluster-size names/OIDs are not validated. The metadata-gated PGLZ decompressor
caps declared output at 64 MiB and returns `54000` above that deliberate safety
bound. Operator-definition DDL's bounded representatives currently refuse with
`0A000` and remain assigned to Q4.

`corpus-regress/` contains PostgreSQL `src/test/regress` SQL files adopted with
`POSTGRES_TAG=REL_18_4 ../../tools/gres-adopt-regress.sh <name>`. Adopted files
keep a provenance header and are attributed in the repository `NOTICE`; do not
hand-copy new files without the script unless the same provenance is preserved.

Pin `POSTGRES_TAG` to the tag matching the conformance oracle — currently
PostgreSQL 18.4, so `REL_18_4`. The script's built-in default is `REL_18_0` and
must be overridden. `boolean` and `int4` were adopted before the oracle moved to
18.4 and still carry `REL_18_0` headers; their upstream sources are
byte-identical at both tags.

[`corpus-regress/TRIAGE.md`](corpus-regress/TRIAGE.md) records the measured
per-file parity and groups the mismatches by root cause; it is the work queue
for the M4 milestone.

`COPY` is routed through its own wire subprotocol rather than `simple_query`:
`COPY ... FROM STDIN` absorbs the inline data block that follows it in a
`pg_regress` file (up to the `\.` terminator) and replays it over copy-in, and
`COPY ... TO STDOUT` is collected over copy-out as one text column per output
line. Sending either down the simple query path instead leaves the connection
in copy mode, which corrupts every later statement in the run *in both
directions* — two dead connections compare equal and score as matches — so this
routing is load-bearing for the measurement, not a convenience.

Server-side `COPY table FROM 'file'` reads PostgreSQL's official fixture files
inside Gres and feeds the bytes through the same atomic text importer. This is
distinct from the wire copy-in state and is required before downstream tests
can exercise the data that `test_setup` creates.

## Relation names must be unique across the whole corpus

Neither engine is reset between corpus files, and the primary corpus runs before
the adopted regress corpus on the same two connections. A relation name reused
with a different definition therefore does not create an independent table: the
second `CREATE TABLE` fails with `42P07` and every later statement in that file
silently runs against the *first* file's schema. Prefix new tables with something
derived from the file name (`setop_a`, `jn_t1`, `msf_m`) and check before adding:

```sh
grep -hoiE 'create (temp |temporary )?table [a-z_0-9]+' \
  crates/gres-conformance/corpus/*.sql crates/gres-conformance/corpus-regress/*/*.sql \
  | tr 'A-Z' 'a-z' | sed -E 's/create (temp |temporary )?table //' | sort | uniq -d
```

Adopted `pg_regress` files are vendored verbatim and cannot be renamed, so the
primary corpus is the side that yields.

Run the adopted corpus with `--corpus-regress crates/gres-conformance/corpus-regress`.
This produces an independent `regress-parity.json`/`regress-parity.md` report in
addition to the normal corpus report. Add
`--regress-baseline crates/gres-conformance/corpus-regress/baseline.json` to gate
the adopted corpus. Unlike `baseline.json`, the regress baseline ratchets per
file as `{file,total,matched}` so early low-parity PostgreSQL files can improve
independently without weakening the existing conformance gate.

When adding or improving an adopted regress file, update the matching entry in
`corpus-regress/baseline.json` in the same review as the SQL/provenance change
and include the generated regress parity report as evidence. File totals are
pinned; matched counts may only increase unless a PostgreSQL 18 oracle drift or
intentional corpus replacement is documented.

Measure against an **empty** oracle database. The harness never resets the
oracle between runs, so tables left behind by an earlier run turn the corpus's
`CREATE TABLE` statements into `42P07` on the oracle side only, and every
statement that depends on them diverges.

## Extended-protocol corpus

`corpus-extended/` contains JSON cases that run through `Parse`/`Bind`/`Execute`
via `tokio-postgres` typed prepared statements instead of the simple query path.
Run it with:

```sh
cargo run -p crabka-gres-conformance -- \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
  --subject-url "host=127.0.0.1 port=5433 user=crab dbname=crab" \
  --baseline crates/gres-conformance/baseline.json \
  --extended-corpus crates/gres-conformance/corpus-extended \
  --extended-baseline crates/gres-conformance/corpus-extended/baseline.json
```

Each `.json` file is discovered recursively and contains cases with `name`,
`sql`, typed `params`, `setup`, and `teardown`; `baseline.json` is the reserved
parity-metadata filename and is not a case file. Supported parameter types are
`int4`, `text`, `bool`, `jsonb`, `int4[]`, and `text[]`; `"value": null` sends a
typed SQL NULL. A `jsonb` value is written as the JSON document's text and bound
in PostgreSQL's binary `jsonb` format; `int4[]`/`text[]` values are JSON arrays
whose elements may be `null`, and `[]` exercises the empty-array binary form.
The F-0 gate covers parameterized `SELECT`, `WHERE`, `INSERT ... RETURNING`,
`= ANY($1)`, and `ON CONFLICT ... DO UPDATE SET col = $n` cases against both
oracle and subject. It writes `extended-parity.json` and
`extended-parity.md`; `--extended-baseline` pins the extended statement total
and ratchets the matched count exactly like the simple corpus baseline.

CI runs this fourteen-case baseline twice: against the standalone subject, producing
`extended-parity-standalone.{json,md}`, and against the substrate-backed subject,
producing `extended-parity-substrate.{json,md}`. Both Markdown summaries are
added to the job summary and all four reports are included in the
`gres-parity-report` artifact.

The front-door gate runs the same corpus through transaction-mode PgDog and
retains `extended-parity-pgdog.{json,md}` under
`target/gres-e2e-artifacts/`. It also executes real parameterized queries in
separate transactions with `tokio-postgres`, `sqlx`, and Python `psycopg`.
Install the pinned Python driver and run the complete gate with:

```sh
python3 -m pip install --require-hashes --no-deps \
  -r crates/gres-conformance/requirements-driver-smoke.txt
CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 ./scripts/gres-e2e.sh
```

Docker/PgDog and `psycopg` are mandatory for the complete gate. For local
engine work that intentionally omits the front door, the explicit
`./scripts/gres-e2e.sh --skip-pgdog` path remains available; it does not provide
PgDog corpus or driver evidence.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
