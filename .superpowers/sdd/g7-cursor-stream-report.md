# G-7 cursor stream checkpoint

Status: partial. Cursor contract and local/registry adapters are implemented;
ordinary SELECT consumption, blocking-operator budgets, and end-to-end transport
streaming are not closed by this checkpoint.

## Layer A RED/GREEN

RED:

`cargo test -p crabka-pgexec cursor_contract_tests --no-run`

Failed with unresolved imports for `MaterializedRangeCursor` and `RangeCursor`.

GREEN:

- `cargo test -p crabka-pgexec cursor_contract_tests`: 2 passed.
- `cargo test -p crabka-pgexec --test distributed_pushdown`: 31 passed.
- `cargo test -p crabka-pgexec --test transactions`: 36 passed.
- `cargo check -p crabka-pgexec --all-targets`: passed.
- `cargo test -p crabka-gres-ranges forward::tests --lib`: 16 passed.
- `cargo check -p crabka-gres-ranges --all-targets`: passed.
- `cargo fmt --all -- --check`: passed (stable rustfmt printed the repository's
  existing nightly-option warnings).
- `git diff --check`: passed.

Commits:

- `db3c228d feat(pgexec): add bounded range cursor contract`
- `8bf396df feat(pgexec): page local MVCC range scans`
- `89f71cfd feat(gres-ranges): page registry scatter scans`
- `03ece16f fix(pgexec): terminate unbounded local scan cursors`

The public pull cursor is async, bounded by requested rows, naturally
backpressured, and lifetime-parametric so native implementations may retain the
statement snapshots. The compatibility cursor is explicitly named
`MaterializedRangeCursor`. Local ordinary scans use bounded row-key ranges and
the durable next-rowid as their terminal bound. Registry scatter scans request a
bounded rowid interval from every owner and merge only that interval.

## Open work

- `query_to_relation` and `execute_read` remain synchronous and build a complete
  `Relation`; `SqlSession::simple_query_into` still reaches the sink only after
  `run_one` returns a complete `QueryResult`.
- Registry cursor termination for an unbounded interval still needs an
  owner-provided terminal rowid/cursor token; it currently cannot truthfully
  mark the last page before `u64::MAX`.
- Blocking operators have no new centralized 53200 budget or spill layer.
- `HostedRangeService` scan responses remain request-materialized rather than a
  connection-owned framed cursor.

