# Authenticated durable-record inspection

## Security and authority

`InspectDurableRecords` is available only through the existing mutually-authenticated range RPC
listener and uses the tenant range-peer allowlist. Operator-only destructive principals and
unknown principals cannot invoke the service. The live implementation checks tenant, hosted
range, immutable WAL generation, table-primary namespace, and half-open physical interval before
reading. It restores an isolated verified checkpoint and replays only `READ_COMMITTED` WAL through
one sampled offset; the mutable SQL/cache KV is never consulted.

The returned bytes are exact. Each surviving record carries its last durable WAL offset/journal
revision, or the verified checkpoint covered offset/journal sequence. Timestamp intent and
prewrite sidecars are structurally decoded and scoped to the requested table interval. TXD2
descriptors are decoded and returned only when their operations are table-local and overlap the
interval. Malformed metadata and mixed-table matching descriptors fail closed.

## Bounds and pagination

Requests are capped at 4,096 records and 128 KiB of raw key/value bytes, keeping JSON responses
below the 1 MiB framed transport limit. The isolated source fold retains its independent input
limits. A four-second service deadline bounds checkpoint/WAL work, while transport cancellation
drops the future. Records are sorted by raw key. Continuation cursors bind the request digest,
sampled committed offset, and last key; later pages reconstruct the same authoritative snapshot.
Wrong tenant/range/generation/table/interval/cursor and an individual record exceeding the page
cap are rejected.

## Verification

- `cargo test -p crabka-gres-substrate readonly_fold::tests --lib --no-fail-fast`: 7 passed.
- `cargo test -p crabka-gres-ranges --lib --no-fail-fast`: 176 passed.
- `cargo test -p crabka-gres --lib --no-fail-fast`: 85 passed.
- `cargo test -p crabka-gres --test topology_process_split_crash --no-run`: process harness and
  authenticated inspection helper compiled.
- `cargo test -p crabka-gres-substrate --all-targets --no-fail-fast`: library 131 and every
  affected substrate target passed; the four pre-existing `raw_kv_split_runtime` failures remain.
- `cargo test -p crabka-gres-ranges --all-targets --no-fail-fast`: library 176 and the affected
  integration suites passed; unrelated pre-existing live-process readiness/TLS/fence failures
  remain in several broad all-target binaries.
- `git diff --check`: clean.

`crates/gres-ranges/src/control.rs` remained the protected pre-existing 14-addition/7-deletion
working-tree delta and is excluded from this commit.
