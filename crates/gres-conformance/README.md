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
`current_database()` can never match, and the replicated engine refuses
sequence advances; both are enumerated there rather than absorbed into
`baseline.json`.

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
