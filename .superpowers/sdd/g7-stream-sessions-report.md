# G-7 stream/session implementation report

## Status

Partial checkpoint only. Commit `addf7925` adds the backpressured result-sink
contract and a specialized `SqlSession` producer. It does **not** close the G-7
Important finding yet: `execute_read` still builds a complete `Relation` and
`QueryResult` before `SqlSession::simple_query_into` drains it into pages.

## RED evidence

Command:

`cargo test -p crabka-pgexec bounded_result_sink_matches_collecting_simple_query --lib`

Expected failure observed: unresolved `ResultPage` and `CollectingResultSink`,
and no `Session::simple_query_into` method.

## GREEN evidence

- `cargo test -p crabka-pgwire --lib` passed.
- `cargo test -p crabka-pgexec bounded_result_sink --lib` passed 2 tests.
- `cargo fmt --all -- --check` passed (stable rustfmt printed the repository's
  existing warnings about nightly-only formatting options).
- `git diff --check` passed.

The tests pin collecting-API semantic parity, bounded page shape, and sink-error
propagation before a later simple-query statement can execute.

## Remaining required work

- Incremental KV/range scanning and projection into the sink.
- Explicit bounded/spill handling for ORDER BY, DISTINCT, aggregation, joins,
  CTEs, set operations, and locking queries, with SQLSTATE 54000 teeth.
- Single negotiated cell encoding, including mixed per-column extended formats.
- Connection-owned authenticated remote sessions and the complete extended
  protocol/lifecycle surface.
- Direct remote simple-query consumption of the execution sink, oversize-row and
  disconnect/cancellation tests, focused suites, and final clean-tree evidence.
