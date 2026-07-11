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
`../../tools/gres-adopt-regress.sh <name>`. Adopted files keep a provenance
header and are attributed in the repository `NOTICE`; do not hand-copy new
files without the script unless the same provenance is preserved.

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
`sql`, typed `params`, `setup`, and `teardown`. Supported parameter types are
`int4`, `text`, and `bool`; `"value": null` sends a typed SQL NULL. The F-0
gate covers parameterized `SELECT`, `WHERE`, and `INSERT ... RETURNING` cases
against both oracle and subject. It writes `extended-parity.json` and
`extended-parity.md`; `--extended-baseline` pins the extended statement total and
ratchets the matched count exactly like the simple corpus baseline.

CI runs this six-case baseline twice: against the standalone subject, producing
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
