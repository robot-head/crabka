# crabka-gres-ranges

Internal range-map and deterministic-routing primitives for Chapter Gres multi-range tenants.

## Gateway capabilities

- A gateway may forward data-range requests to a remote range through the TLS range registry.
- A gateway that does not host range 0 consumes its read-only range-0 follower
  tail for catalog/global visibility and resolves range-0 write RPCs through the
  authenticated registry. Global xid allocation and immutable decisions are
  remote-capable; freshness remains certified by the follower's log-derived
  barrier before reads are served.

## Empty-table local SQL split bridge

`MultiRangeTenant::split_empty_table_successor` is an in-process first slice only. It atomically
publishes a ready local successor engine and table-boundary route map after successor setup, then
new SQL sessions route the moved table to that successor. Existing sessions must reconnect after
the map epoch changes.

The bridge intentionally rejects remote layouts, explicit transactions, indexes, hash/timestamp-
sharded or foreign tables, physical rows, and advanced row-id allocators. It does **not** migrate
nonempty physical SQL data.

## G-9a proof gate

Run the TSO crash/fence model with:

```sh
cargo test -p crabka-gres-ranges --test tso_monotonicity_model
```

This gate exhaustively traverses its finite two-client configuration: two grants,
delayed requests and replies, crash recovery, and live-zombie fencing. It proves
each client's visible timestamp never falls below the maximum of every reply
delivered to that client. It has no checker state-count or wall-clock cutoff; the
test reports the completed traversal counts and fails if the checker does not
complete. Broker-kill bank/Elle coverage and multi-range scaling measurements
remain separate G-9a gates.
